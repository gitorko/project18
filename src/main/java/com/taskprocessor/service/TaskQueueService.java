package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.*;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.metrics.TaskMetrics;
import com.taskprocessor.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
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

    /**
     * Enqueue a single task
     */
    @Transactional
    public TaskResponse enqueueTask(CreateTaskRequest request) {
        Task task = Task.builder()
                .taskType(request.getTaskType())
                .payload(request.getPayload())
                .priority(request.getPriority() != null ? request.getPriority() : 0)
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
                        .taskType(req.getTaskType())
                        .payload(req.getPayload())
                        .priority(req.getPriority() != null ? req.getPriority() : 0)
                        .maxAttempts(req.getMaxAttempts() != null ? req.getMaxAttempts() : 1)
                        .timeoutSeconds(req.getTimeoutSeconds() != null
                                ? req.getTimeoutSeconds()
                                : properties.getPoller().getDefaultTaskTimeoutSeconds())
                        .status(req.getScheduledAt() != null && req.getScheduledAt().isAfter(Instant.now())
                                ? TaskStatus.SCHEDULED : TaskStatus.PENDING)
                        .scheduledAt(req.getScheduledAt())
                        .build())
                .collect(Collectors.toList());

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
        int candidatePoolSize = Math.min(
                batchSize * properties.getPoller().getFairnessCandidateMultiplier(),
                properties.getPoller().getMaxFairnessCandidates());

        List<Task> candidates = taskRepository.findTasksForProcessing(Instant.now(), candidatePoolSize);

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        List<Task> selected = selectFairBatch(candidates, batchSize);

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
                selected.size(), workerId, candidates.size());
        return selected;
    }

    /**
     * Select up to batchSize tasks from the candidate pool, round-robin by task type
     * within each priority tier. The candidate list is already ordered by
     * priority DESC, created_at ASC, so grouping in encounter order naturally yields
     * priority tiers from highest to lowest, and per-type FIFO order within each tier.
     */
    private List<Task> selectFairBatch(List<Task> candidates, int batchSize) {
        LinkedHashMap<Integer, LinkedHashMap<String, ArrayDeque<Task>>> byPriority = new LinkedHashMap<>();
        for (Task task : candidates) {
            byPriority
                    .computeIfAbsent(task.getPriority(), p -> new LinkedHashMap<>())
                    .computeIfAbsent(task.getTaskType(), t -> new ArrayDeque<>())
                    .addLast(task);
        }

        List<Task> result = new ArrayList<>(Math.min(batchSize, candidates.size()));

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
                    Task next = queue.pollFirst();
                    if (next != null) {
                        result.add(next);
                        progressed = true;
                    }
                }
            }
        }

        return result;
    }

    /**
     * Mark a task as completed
     */
    @Transactional
    public void completeTask(UUID taskId) {
        taskRepository.findById(taskId).ifPresent(task -> {
            // Guards against a late write racing with the timeout watchdog (which may
            // have already moved this task to PENDING/FAILED and possibly had it
            // re-dequeued by another worker) or a stale duplicate completion signal.
            if (task.getStatus() != TaskStatus.PROCESSING) {
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
    public void failTask(UUID taskId, String errorMessage) {
        taskRepository.findById(taskId).ifPresent(task -> {
            if (task.getStatus() != TaskStatus.PROCESSING) {
                return;
            }
            if (task.getAttemptCount() < task.getMaxAttempts()) {
                // Schedule for retry
                task.setStatus(TaskStatus.PENDING);
                task.setWorkerId(null);
                task.setLastHeartbeat(null);
                task.setErrorMessage(errorMessage);
                log.info("Task {} failed, scheduling retry (attempt {}/{})", 
                        taskId, task.getAttemptCount(), task.getMaxAttempts());
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

    /**
     * Get task by ID
     */
    @Transactional(readOnly = true)
    public Optional<TaskResponse> getTask(UUID taskId) {
        return taskRepository.findById(taskId)
                .map(TaskResponse::fromEntity);
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
        long pending = taskRepository.countByStatus(TaskStatus.PENDING);
        long processing = taskRepository.countByStatus(TaskStatus.PROCESSING);
        long completed = taskRepository.countByStatus(TaskStatus.COMPLETED);
        long failed = taskRepository.countByStatus(TaskStatus.FAILED);
        long cancelled = taskRepository.countByStatus(TaskStatus.CANCELLED);
        long scheduled = taskRepository.countByStatus(TaskStatus.SCHEDULED);

        // Get pending tasks by type
        Map<String, Long> pendingByType = new HashMap<>();
        taskRepository.countByTaskTypeAndStatus(TaskStatus.PENDING)
                .forEach(row -> pendingByType.put((String) row[0], (Long) row[1]));

        return TaskSummaryResponse.builder()
                .totalTasks(pending + processing + completed + failed + cancelled + scheduled)
                .pendingTasks(pending)
                .processingTasks(processing)
                .completedTasks(completed)
                .failedTasks(failed)
                .cancelledTasks(cancelled)
                .scheduledTasks(scheduled)
                .pendingByType(pendingByType)
                .processingRatePerMinute(taskMetrics.getProcessingRatePerMinute())
                .avgProcessingTimeMs(taskMetrics.getAverageProcessingTimeMs())
                .activeWorkers(taskMetrics.getActiveWorkerCount())
                .currentPoolSize(taskMetrics.getCurrentPoolSize())
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
