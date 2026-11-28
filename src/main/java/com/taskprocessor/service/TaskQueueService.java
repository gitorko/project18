package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.*;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskPriority;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.metrics.TaskMetrics;
import com.taskprocessor.repository.TaskRepository;
import com.taskprocessor.repository.TaskIdempotencyRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for managing the task queue.
 * Provides operations for enqueueing, dequeueing, and querying tasks.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskQueueService {

    private final TaskRepository taskRepository;
    private final TaskProcessorProperties properties;
    private final TaskMetrics taskMetrics;
    private final QueueAdmissionService queueAdmissionService;
    private final TaskIdempotencyRepository idempotencyRepository;

    /** How many times dequeue may look deeper in the queue when capped types crowd out other work. */
    private static final int MAX_CANDIDATE_REFETCHES = 3;

    /**
     * Enqueue a single task
     */
    @Transactional
    public TaskResponse enqueueTask(CreateTaskRequest request) {
        return enqueueTask(request, null);
    }

    @Transactional
    public TaskResponse enqueueTask(CreateTaskRequest request, String idempotencyKey) {
        Task task = Task.builder()
                .id(UUID.randomUUID())
                .taskType(request.getTaskType())
                .payload(request.getPayload())
                .priority(request.getPriority() != null ? request.getPriority() : TaskPriority.MEDIUM)
                // Retries must be explicitly opted into at creation time; otherwise
                // a failure (including a timeout) fails the task immediately.
                .maxAttempts(request.getMaxAttempts() != null ? request.getMaxAttempts() : 1)
                .timeoutSeconds(request.getTimeoutSeconds() != null
                        ? request.getTimeoutSeconds()
                        : properties.getPoller().getDefaultTaskTimeoutSeconds())
                .status(request.getScheduledAt() != null && request.getScheduledAt().isAfter(Instant.now())
                        ? TaskStatus.SCHEDULED : TaskStatus.PENDING)
                .scheduledAt(request.getScheduledAt())
                .build();

        if (idempotencyKey != null) {
            if (idempotencyKey.isBlank() || idempotencyKey.length() > 255) {
                throw new IllegalArgumentException("Idempotency-Key must contain 1 to 255 characters");
            }
            String requestHash = fingerprint(task);
            int reserved = idempotencyRepository.reserve(idempotencyKey, requestHash, task.getId());
            if (reserved == 0) {
                var existingRecord = idempotencyRepository.findById(idempotencyKey)
                        .orElseThrow(() -> new IllegalStateException("Idempotency record disappeared"));
                if (!existingRecord.getRequestHash().equals(requestHash)) {
                    throw new IdempotencyConflictException(
                            "Idempotency-Key was already used for a different task request");
                }
                return taskRepository.findById(existingRecord.getTaskId())
                        .map(TaskResponse::fromEntity)
                        .orElseThrow(() -> new IllegalStateException("Idempotent task record is missing"));
            }
        }

        queueAdmissionService.assertCanAccept(List.of(task));

        task = taskRepository.save(task);
        taskMetrics.recordTaskEnqueued(task.getTaskType());
        
        log.debug("Enqueued task: id={}, type={}", task.getId(), task.getTaskType());
        return TaskResponse.fromEntity(task);
    }

    /**
     * Enqueue multiple tasks in a batch
     */
    @Transactional
    public List<TaskResponse> enqueueTasks(BulkCreateTaskRequest request) {
        List<Task> tasks = request.getTasks().stream()
                .map(req -> Task.builder()
                        .id(UUID.randomUUID())
                        .taskType(req.getTaskType())
                        .payload(req.getPayload())
                        .priority(req.getPriority() != null ? req.getPriority() : TaskPriority.MEDIUM)
                        .maxAttempts(req.getMaxAttempts() != null ? req.getMaxAttempts() : 1)
                        .timeoutSeconds(req.getTimeoutSeconds() != null
                                ? req.getTimeoutSeconds()
                                : properties.getPoller().getDefaultTaskTimeoutSeconds())
                        .status(req.getScheduledAt() != null && req.getScheduledAt().isAfter(Instant.now())
                                ? TaskStatus.SCHEDULED : TaskStatus.PENDING)
                        .scheduledAt(req.getScheduledAt())
                        .build())
                .collect(Collectors.toList());

        queueAdmissionService.assertCanAccept(tasks);

        tasks = taskRepository.saveAll(tasks);
        
        tasks.forEach(task -> taskMetrics.recordTaskEnqueued(task.getTaskType()));
        
        log.info("Enqueued {} tasks in batch", tasks.size());
        return tasks.stream()
                .map(TaskResponse::fromEntity)
                .collect(Collectors.toList());
    }

    /**
     * Dequeue tasks for processing using SKIP LOCKED.
     * This is the core of the distributed queue - ensures no two workers get the same task.
     *
     * Priority still strictly wins across tiers (a lower-priority task type is never
     * picked while any higher-priority task is pending), but within a single priority
     * tier, task types are selected round-robin instead of pure FIFO - so one heavily
     * enqueued type can't monopolize every poll cycle at the expense of another type
     * queued at the same priority.
     */
    @Transactional
    public List<Task> dequeueTasks(int batchSize) {
        return dequeueTasks(batchSize, Collections.emptyMap());
    }

    @Transactional
    public List<Task> dequeueTasks(int batchSize, Map<String, Integer> availableCapacityByType) {
        int candidatePoolSize = Math.min(
                batchSize * properties.getPoller().getFairnessCandidateMultiplier(),
                properties.getPoller().getMaxFairnessCandidates());

        // Types with no capacity left are excluded in SQL so they cannot fill the candidate
        // pool and starve other types of work.
        Set<String> excludedTypes = new LinkedHashSet<>();
        availableCapacityByType.forEach((type, capacity) -> {
            if (capacity <= 0) {
                excludedTypes.add(type);
            }
        });

        List<Task> selected = new ArrayList<>();
        Set<UUID> seen = new HashSet<>();
        int totalCandidates = 0;

        for (int pass = 0; pass <= MAX_CANDIDATE_REFETCHES; pass++) {
            List<Task> candidates = excludedTypes.isEmpty()
                    ? taskRepository.findTasksForProcessing(Instant.now(), candidatePoolSize)
                    : taskRepository.findTasksForProcessingExcludingTypes(
                            Instant.now(), candidatePoolSize, excludedTypes);
            totalCandidates += candidates.size();

            // Rows locked by this transaction are returned again on a refetch; skip them.
            List<Task> fresh = new ArrayList<>(candidates.size());
            for (Task candidate : candidates) {
                if (seen.add(candidate.getId())) {
                    fresh.add(candidate);
                }
            }

            selected.addAll(selectFairBatch(fresh, batchSize - selected.size(),
                    remainingCapacity(availableCapacityByType, selected)));

            if (selected.size() >= batchSize || candidates.size() < candidatePoolSize) {
                break;
            }
            // The pool was full yet the batch is not: types that just hit their cap may be
            // crowding out runnable work deeper in the queue. Exclude them and look again.
            Map<String, Integer> remaining = remainingCapacity(availableCapacityByType, selected);
            boolean grew = false;
            for (var entry : remaining.entrySet()) {
                if (entry.getValue() <= 0 && excludedTypes.add(entry.getKey())) {
                    grew = true;
                }
            }
            if (!grew) {
                break;
            }
        }

        if (selected.isEmpty()) {
            return Collections.emptyList();
        }

        Instant now = Instant.now();
        String workerId = properties.getWorkerId();

        for (Task task : selected) {
            task.setStatus(TaskStatus.PROCESSING);
            task.setWorkerId(workerId);
            task.setStartedAt(now);
            task.setLastHeartbeat(now);
            task.setAttemptCount(task.getAttemptCount() + 1);
        }

        selected = taskRepository.saveAll(selected);

        log.debug("Dequeued {} tasks for processing by worker {} (from {} candidates)",
                selected.size(), workerId, totalCandidates);
        return selected;
    }

    /**
     * Select up to batchSize tasks from the candidate pool, round-robin by task type
     * within each priority tier. The candidate list is already ordered by
     * priority DESC, created_at ASC, so grouping in encounter order naturally yields
     * priority tiers from highest to lowest, and per-type FIFO order within each tier.
     */
    private List<Task> selectFairBatch(List<Task> candidates, int batchSize,
                                       Map<String, Integer> availableCapacityByType) {
        LinkedHashMap<TaskPriority, LinkedHashMap<String, ArrayDeque<Task>>> byPriority = new LinkedHashMap<>();
        for (Task task : candidates) {
            byPriority
                    .computeIfAbsent(task.getPriority(), p -> new LinkedHashMap<>())
                    .computeIfAbsent(task.getTaskType(), t -> new ArrayDeque<>())
                    .addLast(task);
        }

        List<Task> result = new ArrayList<>(Math.min(batchSize, candidates.size()));
        Map<String, Integer> selectedByType = new HashMap<>();

        for (LinkedHashMap<String, ArrayDeque<Task>> byType : byPriority.values()) {
            if (result.size() >= batchSize) {
                break;
            }
            // Round-robin across task types within this priority tier until the tier
            // is drained or the batch is full - only then does the next (lower)
            // priority tier get considered.
            boolean progressed = true;
            while (result.size() < batchSize && progressed) {
                progressed = false;
                for (ArrayDeque<Task> queue : byType.values()) {
                    if (result.size() >= batchSize) {
                        break;
                    }
                    Task head = queue.peekFirst();
                    if (head == null) {
                        continue;
                    }
                    int typeCapacity = availableCapacityByType.getOrDefault(head.getTaskType(), batchSize);
                    if (selectedByType.getOrDefault(head.getTaskType(), 0) >= typeCapacity) {
                        continue;
                    }
                    Task next = queue.pollFirst();
                    if (next != null) {
                        result.add(next);
                        selectedByType.merge(next.getTaskType(), 1, Integer::sum);
                        progressed = true;
                    }
                }
            }
        }

        return result;
    }

    /** Capacity left per capped type after accounting for tasks already selected in this dequeue. */
    private Map<String, Integer> remainingCapacity(Map<String, Integer> availableCapacityByType,
                                                   List<Task> selected) {
        if (availableCapacityByType.isEmpty()) {
            return availableCapacityByType;
        }
        Map<String, Integer> remaining = new HashMap<>(availableCapacityByType);
        for (Task task : selected) {
            remaining.computeIfPresent(task.getTaskType(), (type, capacity) -> capacity - 1);
        }
        return remaining;
    }

    private String fingerprint(Task task) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            updateDigest(digest, task.getTaskType());
            updateDigest(digest, task.getPayload());
            updateDigest(digest, task.getPriority().name());
            updateDigest(digest, task.getMaxAttempts().toString());
            updateDigest(digest, task.getTimeoutSeconds().toString());
            updateDigest(digest, task.getScheduledAt() == null ? null : task.getScheduledAt().toString());
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private void updateDigest(MessageDigest digest, String value) {
        if (value == null) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(-1).array());
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    /**
     * Mark a task as completed
     */
    @Transactional
    public void completeTask(UUID taskId, int attemptCount) {
        taskRepository.findByIdForUpdate(taskId).ifPresent(task -> {
            // Guards against a late write racing with the timeout watchdog (which may
            // have already moved this task to PENDING/FAILED and possibly had it
            // re-dequeued by another worker) or a stale duplicate completion signal.
            if (task.getStatus() != TaskStatus.PROCESSING || task.getAttemptCount() != attemptCount) {
                return;
            }
            task.setStatus(TaskStatus.COMPLETED);
            task.setCompletedAt(Instant.now());
            task.setWorkerId(null);
            taskRepository.save(task);
            
            taskMetrics.recordTaskCompleted(task.getTaskType(), 
                    task.getCompletedAt().toEpochMilli() - task.getStartedAt().toEpochMilli());
            
            log.debug("Completed task: id={}", taskId);
        });
    }

    /**
     * Mark a task as failed
     */
    @Transactional
    public void failTask(UUID taskId, int attemptCount, String errorMessage) {
        taskRepository.findByIdForUpdate(taskId).ifPresent(task -> {
            if (task.getStatus() != TaskStatus.PROCESSING || task.getAttemptCount() != attemptCount) {
                return;
            }
            if (task.getAttemptCount() < task.getMaxAttempts()) {
                long delayMs = RetryDelayCalculator.delayMillis(task.getAttemptCount(),
                        properties.getRetry().getInitialDelayMs(), properties.getRetry().getMaxDelayMs(),
                        properties.getRetry().getJitterFactor());
                task.setStatus(TaskStatus.SCHEDULED);
                task.setScheduledAt(Instant.now().plusMillis(delayMs));
                task.setWorkerId(null);
                task.setLastHeartbeat(null);
                task.setErrorMessage(errorMessage);
                log.info("Task {} failed, retry scheduled in {}ms (attempt {}/{})",
                        taskId, delayMs, task.getAttemptCount(), task.getMaxAttempts());
            } else {
                // Max retries exceeded
                task.setStatus(TaskStatus.FAILED);
                task.setCompletedAt(Instant.now());
                task.setWorkerId(null);
                task.setErrorMessage(errorMessage);
                taskMetrics.recordTaskFailed(task.getTaskType());
                log.warn("Task {} failed permanently after {} attempts: {}", 
                        taskId, task.getAttemptCount(), errorMessage);
            }
            taskRepository.save(task);
        });
    }

    /**
     * Update heartbeat for a task being processed
     */
    @Transactional
    public void updateHeartbeat(UUID taskId) {
        taskRepository.updateHeartbeat(taskId, properties.getWorkerId(), Instant.now());
    }

    /** Update heartbeats for all given tasks in a single statement. */
    @Transactional
    public void updateHeartbeats(Collection<UUID> taskIds) {
        if (taskIds.isEmpty()) {
            return;
        }
        taskRepository.updateHeartbeats(taskIds, properties.getWorkerId(), Instant.now());
    }

    /**
     * Return tasks this worker still holds to the queue, e.g. at shutdown or when they could
     * not be handed to the executor. The aborted attempt does not count against retries.
     */
    @Transactional
    public int releaseTasks(Collection<UUID> taskIds) {
        if (taskIds.isEmpty()) {
            return 0;
        }
        return taskRepository.releaseProcessingTasks(taskIds, properties.getWorkerId(), Instant.now());
    }

    /**
     * Get task by ID
     */
    @Transactional(readOnly = true)
    public Optional<TaskResponse> getTask(UUID taskId) {
        return taskRepository.findById(taskId)
                .map(TaskResponse::fromEntity);
    }

    /** Create a fresh execution from a failed task while retaining the failed record. */
    @Transactional
    public Optional<TaskResponse> replayFailedTask(UUID taskId) {
        return taskRepository.findByIdForUpdate(taskId).map(failedTask -> {
            if (failedTask.getStatus() != TaskStatus.FAILED) {
                throw new TaskNotReplayableException("Only failed tasks can be replayed");
            }
            Task replay = Task.builder()
                    .id(UUID.randomUUID())
                    .taskType(failedTask.getTaskType())
                    .payload(failedTask.getPayload())
                    .priority(failedTask.getPriority())
                    .maxAttempts(failedTask.getMaxAttempts())
                    .timeoutSeconds(failedTask.getTimeoutSeconds())
                    .status(TaskStatus.PENDING)
                    .scheduledAt(Instant.now())
                    .replayOfTaskId(failedTask.getId())
                    .build();
            queueAdmissionService.assertCanAccept(List.of(replay));
            Task saved = taskRepository.save(replay);
            taskMetrics.recordTaskEnqueued(saved.getTaskType());
            return TaskResponse.fromEntity(saved);
        });
    }

    /**
     * Get paginated list of tasks
     */
    @Transactional(readOnly = true)
    public Page<TaskResponse> getTasks(TaskStatus status, Pageable pageable) {
        Page<Task> tasks;
        if (status != null) {
            tasks = taskRepository.findByStatusOrderByPriorityDescCreatedAtAsc(status, pageable);
        } else {
            tasks = taskRepository.findAllByOrderByCreatedAtDesc(pageable);
        }
        return tasks.map(TaskResponse::fromEntity);
    }

    /**
     * Get task summary statistics
     */
    @Transactional(readOnly = true)
    public TaskSummaryResponse getTaskSummary() {
        // One grouped query yields every per-status total, per-type breakdown and the
        // pending-by-type map, instead of a separate COUNT per status.
        Map<TaskStatus, Long> totals = new EnumMap<>(TaskStatus.class);
        Map<String, Long> pendingByType = new HashMap<>();
        Map<String, TaskTypeSummary> byTaskType = new TreeMap<>();
        taskRepository.countByTaskTypeAndStatus().forEach(row -> {
            String type = (String) row[0];
            TaskStatus status = (TaskStatus) row[1];
            long count = (Long) row[2];
            totals.merge(status, count, Long::sum);
            TaskTypeSummary current = byTaskType.computeIfAbsent(type, ignored -> TaskTypeSummary.builder().build());
            switch (status) {
                case PENDING -> {
                    current.setPendingTasks(count);
                    pendingByType.put(type, count);
                }
                case SCHEDULED -> current.setScheduledTasks(count);
                case PROCESSING -> current.setProcessingTasks(count);
                case COMPLETED -> current.setCompletedTasks(count);
                case FAILED -> current.setFailedTasks(count);
                case CANCELLED -> current.setCancelledTasks(count);
                default -> { }
            }
        });
        byTaskType.forEach((type, stats) -> stats.setAvgProcessingTimeMs(taskMetrics.getAverageProcessingTimeMs(type)));

        long pending = totals.getOrDefault(TaskStatus.PENDING, 0L);
        long processing = totals.getOrDefault(TaskStatus.PROCESSING, 0L);
        long completed = totals.getOrDefault(TaskStatus.COMPLETED, 0L);
        long failed = totals.getOrDefault(TaskStatus.FAILED, 0L);
        long cancelled = totals.getOrDefault(TaskStatus.CANCELLED, 0L);
        long scheduled = totals.getOrDefault(TaskStatus.SCHEDULED, 0L);

        long outstandingTasks = pending + processing;
        double averageMs = taskMetrics.getAverageProcessingTimeMs();
        int workerCapacity = Math.max(1, taskMetrics.getConfiguredPoolSize());
        Long estimatedBacklogCompletionSeconds = averageMs > 0
                ? (long) Math.ceil((outstandingTasks * averageMs) / workerCapacity / 1000.0)
                : null;

        return TaskSummaryResponse.builder()
                .totalTasks(pending + processing + completed + failed + cancelled + scheduled)
                .pendingTasks(pending)
                .processingTasks(processing)
                .completedTasks(completed)
                .failedTasks(failed)
                .cancelledTasks(cancelled)
                .scheduledTasks(scheduled)
                .pendingByType(pendingByType)
                .byTaskType(byTaskType)
                .processingRatePerMinute(taskMetrics.getProcessingRatePerMinute())
                .avgProcessingTimeMs(taskMetrics.getAverageProcessingTimeMs())
                .activeWorkers(taskMetrics.getActiveWorkerCount())
                .currentPoolSize(taskMetrics.getCurrentPoolSize())
                .estimatedBacklogCompletionSeconds(estimatedBacklogCompletionSeconds)
                .build();
    }

    /**
     * Cancel a task
     */
    @Transactional
    public boolean cancelTask(UUID taskId) {
        return taskRepository.findById(taskId)
                .filter(task -> task.getStatus() == TaskStatus.PENDING || 
                               task.getStatus() == TaskStatus.SCHEDULED)
                .map(task -> {
                    task.setStatus(TaskStatus.CANCELLED);
                    taskRepository.save(task);
                    log.info("Cancelled task: id={}", taskId);
                    return true;
                })
                .orElse(false);
    }

    /**
     * Get tasks currently being processed by this worker
     */
    @Transactional(readOnly = true)
    public List<TaskResponse> getProcessingTasks() {
        return taskRepository.findByWorkerIdAndStatus(properties.getWorkerId(), TaskStatus.PROCESSING)
                .stream()
                .map(TaskResponse::fromEntity)
                .collect(Collectors.toList());
    }
}
