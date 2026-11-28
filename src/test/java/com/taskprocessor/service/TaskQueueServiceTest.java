package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.CreateTaskRequest;
import com.taskprocessor.dto.TaskResponse;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskPriority;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.entity.TaskIdempotencyRecord;
import com.taskprocessor.metrics.TaskMetrics;
import com.taskprocessor.repository.TaskIdempotencyRepository;
import com.taskprocessor.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskQueueServiceTest {
    @Mock TaskRepository repository;
    @Mock TaskMetrics metrics;
    @Mock QueueAdmissionService admissionService;
    @Mock TaskIdempotencyRepository idempotencyRepository;
    private TaskProcessorProperties properties;
    private TaskQueueService service;

    @BeforeEach void setUp() {
        properties = new TaskProcessorProperties();
        properties.setWorkerId("worker-1");
        service = new TaskQueueService(repository, properties, metrics, admissionService, idempotencyRepository);
    }

    @Test void enqueueUsesDefaultsAndTracksMetric() {
        when(repository.save(any(Task.class))).thenAnswer(inv -> {
            Task task = inv.getArgument(0); task.setId(UUID.randomUUID()); task.setCreatedAt(Instant.now()); return task;
        });
        TaskResponse result = service.enqueueTask(CreateTaskRequest.builder().taskType("mail").build());
        ArgumentCaptor<Task> captor = ArgumentCaptor.forClass(Task.class);
        verify(repository).save(captor.capture());
        assertEquals(TaskPriority.MEDIUM, captor.getValue().getPriority());
        assertEquals(1, captor.getValue().getMaxAttempts());
        assertEquals(properties.getPoller().getDefaultTaskTimeoutSeconds(), captor.getValue().getTimeoutSeconds());
        assertEquals(TaskStatus.PENDING, captor.getValue().getStatus());
        assertEquals("mail", result.getTaskType());
        verify(metrics).recordTaskEnqueued("mail");
    }

    @Test void enqueueFutureTaskAsScheduledAndPastTaskAsPending() {
        when(repository.save(any(Task.class))).thenAnswer(inv -> inv.getArgument(0));
        TaskResponse scheduled = service.enqueueTask(CreateTaskRequest.builder()
                .taskType("mail").scheduledAt(Instant.now().plusSeconds(60)).build());
        TaskResponse immediate = service.enqueueTask(CreateTaskRequest.builder()
                .taskType("mail").scheduledAt(Instant.now().minusSeconds(60)).build());
        assertEquals(TaskStatus.SCHEDULED, scheduled.getStatus());
        assertEquals(TaskStatus.PENDING, immediate.getStatus());
    }

    /** Candidate row as returned by the narrow candidate query (no payload, no other columns). */
    private static TaskRepository.CandidateRow row(Task task) {
        return new TaskRepository.CandidateRow() {
            @Override public UUID getId() { return task.getId(); }
            @Override public String getTaskType() { return task.getTaskType(); }
            @Override public Integer getPriority() { return task.getPriority().getDatabaseValue(); }
        };
    }

    private static List<TaskRepository.CandidateRow> rows(Task... tasks) {
        return java.util.Arrays.stream(tasks).map(TaskQueueServiceTest::row).toList();
    }

    /** Simulates the claim statement: returns the requested rows, in arbitrary (reversed) order. */
    private void stubClaimFor(Task... all) {
        when(repository.claimTasks(anyCollection(), eq("worker-1"), any())).thenAnswer(inv -> {
            java.util.Collection<UUID> ids = inv.getArgument(1 - 1);
            List<Task> claimed = new java.util.ArrayList<>();
            for (Task t : all) {
                if (ids.contains(t.getId())) {
                    t.setStatus(TaskStatus.PROCESSING);
                    t.setWorkerId("worker-1");
                    t.setAttemptCount(t.getAttemptCount() + 1);
                    t.setScheduledAt(Instant.now().minusMillis(40));
                    t.setStartedAt(Instant.now());
                    claimed.add(t);
                }
            }
            java.util.Collections.reverse(claimed);
            return claimed;
        });
    }

    @Test void dequeueClaimsOnlySelectedRowsAndRoundRobinsWithinPriority() {
        Task a1 = task("a", TaskPriority.MEDIUM, 10), a2 = task("a", TaskPriority.MEDIUM, 10),
                b1 = task("b", TaskPriority.MEDIUM, 10), low = task("low", TaskPriority.LOW, 10);
        when(repository.findCandidates(any(), anyInt())).thenReturn(rows(a1, a2, b1, low));
        stubClaimFor(a1, a2, b1, low);

        List<Task> result = service.dequeueTasks(3);

        assertEquals(List.of(a1, b1, a2), result, "result keeps the fair selection order");
        assertTrue(result.stream().allMatch(t -> t.getStatus() == TaskStatus.PROCESSING));
        assertEquals(TaskStatus.PENDING, low.getStatus());
        verify(repository).findCandidates(any(), eq(15));
        ArgumentCaptor<java.util.Collection<UUID>> claimed = ArgumentCaptor.forClass(java.util.Collection.class);
        verify(repository).claimTasks(claimed.capture(), eq("worker-1"), any());
        assertEquals(List.of(a1.getId(), b1.getId(), a2.getId()), List.copyOf(claimed.getValue()));
        verify(repository, never()).saveAll(anyList());
        verify(metrics, times(3)).recordQueueWait(anyString(), longThat(wait -> wait >= 0));
    }

    @Test void dequeueReturnsEmptyWhenNoCandidates() {
        when(repository.findCandidates(any(), anyInt())).thenReturn(List.of());
        assertTrue(service.dequeueTasks(4).isEmpty());
        verify(repository, never()).claimTasks(any(), any(), any());
    }

    @Test void typeCapacitySkipsCappedTypeAndUsesOtherAvailableTypes() {
        Task capped = task("heavy", TaskPriority.HIGH, 1);
        Task other = task("fast", TaskPriority.HIGH, 1);
        when(repository.findCandidatesExcludingTypes(any(), anyInt(), eq(java.util.Set.of("heavy"))))
                .thenReturn(rows(other));
        stubClaimFor(capped, other);

        List<Task> selected = service.dequeueTasks(1, java.util.Map.of("heavy", 0));

        assertEquals(List.of(other), selected);
        assertEquals(TaskStatus.PENDING, capped.getStatus());
        verify(repository, never()).findCandidates(any(), anyInt());
    }

    @Test void saturatedTypeCannotCrowdOutOtherTypesFromTheCandidatePool() {
        // batch 3 -> pool of 15, entirely filled by the capped type
        List<Task> heavy = java.util.stream.IntStream.range(0, 15)
                .mapToObj(i -> task("heavy", TaskPriority.HIGH, 1)).toList();
        Task fast1 = task("fast", TaskPriority.MEDIUM, 1), fast2 = task("fast", TaskPriority.MEDIUM, 1);
        when(repository.findCandidates(any(), anyInt())).thenReturn(heavy.stream().map(TaskQueueServiceTest::row).toList());
        when(repository.findCandidatesExcludingTypes(any(), anyInt(), eq(java.util.Set.of("heavy"))))
                .thenReturn(rows(fast1, fast2));
        List<Task> all = new java.util.ArrayList<>(heavy);
        all.add(fast1);
        all.add(fast2);
        stubClaimFor(all.toArray(new Task[0]));

        List<Task> selected = service.dequeueTasks(3, java.util.Map.of("heavy", 1));

        assertEquals(List.of(heavy.get(0), fast1, fast2), selected);
    }

    @Test void refetchSkipsRowsAlreadyConsideredInAnEarlierPass() {
        List<Task> heavy = java.util.stream.IntStream.range(0, 15)
                .mapToObj(i -> task("heavy", TaskPriority.HIGH, 1)).toList();
        Task fast = task("fast", TaskPriority.MEDIUM, 1);
        when(repository.findCandidates(any(), anyInt())).thenReturn(heavy.stream().map(TaskQueueServiceTest::row).toList());
        when(repository.findCandidatesExcludingTypes(any(), anyInt(), any()))
                .thenReturn(rows(heavy.get(0), fast));
        List<Task> all = new java.util.ArrayList<>(heavy);
        all.add(fast);
        stubClaimFor(all.toArray(new Task[0]));

        List<Task> selected = service.dequeueTasks(3, java.util.Map.of("heavy", 1));

        assertEquals(List.of(heavy.get(0), fast), selected);
        assertEquals(1, heavy.get(0).getAttemptCount());
    }

    @Test void updatesHeartbeatsInOneStatement() {
        List<UUID> ids = List.of(UUID.randomUUID(), UUID.randomUUID());
        service.updateHeartbeats(ids);
        verify(repository).updateHeartbeats(eq(ids), eq("worker-1"), any());
        service.updateHeartbeats(List.of());
        verifyNoMoreInteractions(repository);
    }

    @Test void releaseTasksReturnsClaimsToTheQueue() {
        List<UUID> ids = List.of(UUID.randomUUID());
        when(repository.releaseProcessingTasks(eq(ids), eq("worker-1"), any())).thenReturn(1);
        assertEquals(1, service.releaseTasks(ids));
        assertEquals(0, service.releaseTasks(List.of()));
    }

    @Test void completeTaskIsOneGuardedUpdateAndRecordsMetricsOnlyWhenItApplies() {
        Task task = task("x", TaskPriority.MEDIUM, 1);
        task.setStatus(TaskStatus.PROCESSING); task.setAttemptCount(1);
        task.setStartedAt(Instant.now().minusMillis(50));
        when(repository.markCompleted(eq(task.getId()), eq(1), any())).thenReturn(1);
        service.completeTask(task);
        verify(repository).markCompleted(eq(task.getId()), eq(1), any());
        verify(repository, never()).findByIdForUpdate(any());
        verify(repository, never()).save(any());
        verify(metrics).recordTaskCompleted(eq("x"), longThat(duration -> duration >= 50));

        // The guard rejected the update (task no longer PROCESSING for this attempt).
        clearInvocations(repository, metrics);
        when(repository.markCompleted(any(), anyInt(), any())).thenReturn(0);
        service.completeTask(task);
        verifyNoInteractions(metrics);
    }

    @Test void failTaskSchedulesRetryWithBackoffWhileAttemptsRemain() {
        Task task = task("x", TaskPriority.MEDIUM, 2); task.setStatus(TaskStatus.PROCESSING); task.setAttemptCount(1);
        when(repository.scheduleRetry(eq(task.getId()), eq(1), any(), eq("retry me"), any())).thenReturn(1);
        Instant beforeRetry = Instant.now();

        service.failTask(task, "retry me");

        ArgumentCaptor<Instant> retryAt = ArgumentCaptor.forClass(Instant.class);
        verify(repository).scheduleRetry(eq(task.getId()), eq(1), retryAt.capture(), eq("retry me"), any());
        assertTrue(retryAt.getValue().isAfter(beforeRetry));
        assertTrue(retryAt.getValue().isBefore(beforeRetry.plusSeconds(2)));
        verify(repository, never()).markFailed(any(), anyInt(), any(), any());
        verify(metrics).recordTaskRetried("x");
        verify(metrics, never()).recordTaskFailed(any());
    }

    @Test void failTaskMarksExhaustedTaskFailed() {
        Task task = task("x", TaskPriority.MEDIUM, 2); task.setStatus(TaskStatus.PROCESSING); task.setAttemptCount(2);
        when(repository.markFailed(eq(task.getId()), eq(2), eq("terminal"), any())).thenReturn(1);

        service.failTask(task, "terminal");

        verify(repository).markFailed(eq(task.getId()), eq(2), eq("terminal"), any());
        verify(repository, never()).scheduleRetry(any(), anyInt(), any(), any(), any());
        verify(metrics).recordTaskFailed("x");
    }

    @Test void failTaskIgnoresStaleAttemptsRejectedByTheGuard() {
        Task task = task("x", TaskPriority.MEDIUM, 1); task.setAttemptCount(1);
        when(repository.markFailed(any(), anyInt(), any(), any())).thenReturn(0);
        service.failTask(task, "late");
        verifyNoInteractions(metrics);
    }

    @Test void enqueuedTasksCarryTheRequestCorrelationId() {
        org.slf4j.MDC.put(com.taskprocessor.web.CorrelationIdFilter.MDC_KEY, "req-42");
        try {
            when(repository.save(any(Task.class))).thenAnswer(inv -> inv.getArgument(0));
            TaskResponse response = service.enqueueTask(CreateTaskRequest.builder().taskType("mail").build());
            assertEquals("req-42", response.getCorrelationId());
        } finally {
            org.slf4j.MDC.clear();
        }
    }

    @Test void idempotencyKeyReturnsOriginalTaskForTheSameRequest() {
        CreateTaskRequest request = CreateTaskRequest.builder().taskType("mail").payload("{}").build();
        when(idempotencyRepository.reserve(eq("request-1"), anyString(), any()))
                .thenReturn(1, 0);
        when(repository.save(any(Task.class))).thenAnswer(inv -> inv.getArgument(0));
        TaskResponse created = service.enqueueTask(request, "request-1");
        ArgumentCaptor<String> hashCaptor = ArgumentCaptor.forClass(String.class);
        verify(idempotencyRepository).reserve(eq("request-1"), hashCaptor.capture(), any());

        TaskIdempotencyRecord record = new TaskIdempotencyRecord("request-1", hashCaptor.getValue(), created.getId());
        when(idempotencyRepository.findById("request-1")).thenReturn(Optional.of(record));
        Task persisted = Task.builder().id(created.getId()).taskType("mail").payload("{}").build();
        when(repository.findById(created.getId())).thenReturn(Optional.of(persisted));

        TaskResponse repeated = service.enqueueTask(request, "request-1");

        assertEquals(created.getId(), repeated.getId());
        verify(repository, times(1)).save(any(Task.class));
        verify(metrics, times(1)).recordTaskEnqueued("mail");
    }

    @Test void idempotencyKeyCannotBeReusedForDifferentRequestBody() {
        CreateTaskRequest original = CreateTaskRequest.builder().taskType("mail").payload("{}").build();
        when(idempotencyRepository.reserve(eq("request-2"), anyString(), any())).thenReturn(1, 0);
        when(repository.save(any(Task.class))).thenAnswer(inv -> inv.getArgument(0));
        TaskResponse created = service.enqueueTask(original, "request-2");
        ArgumentCaptor<String> hashCaptor = ArgumentCaptor.forClass(String.class);
        verify(idempotencyRepository).reserve(eq("request-2"), hashCaptor.capture(), any());
        when(idempotencyRepository.findById("request-2")).thenReturn(Optional.of(
                new TaskIdempotencyRecord("request-2", hashCaptor.getValue(), created.getId())));

        assertThrows(IdempotencyConflictException.class, () -> service.enqueueTask(
                CreateTaskRequest.builder().taskType("mail").payload("different").build(), "request-2"));
        verify(repository, times(1)).save(any(Task.class));
    }

    @Test void replayCreatesNewPendingTaskLinkedToFailedTask() {
        Task failed = task("mail", TaskPriority.HIGH, 3);
        failed.setStatus(TaskStatus.FAILED);
        when(repository.findByIdForUpdate(failed.getId())).thenReturn(Optional.of(failed));
        when(repository.save(any(Task.class))).thenAnswer(inv -> {
            Task task = inv.getArgument(0);
            task.setId(UUID.randomUUID());
            return task;
        });

        TaskResponse replay = service.replayFailedTask(failed.getId()).orElseThrow();

        ArgumentCaptor<Task> captor = ArgumentCaptor.forClass(Task.class);
        verify(repository).save(captor.capture());
        assertEquals(TaskStatus.PENDING, captor.getValue().getStatus());
        assertEquals(failed.getId(), captor.getValue().getReplayOfTaskId());
        assertEquals(TaskPriority.HIGH, replay.getPriority());
        assertEquals(failed.getId(), replay.getReplayOfTaskId());
    }

    @Test void cancelOnlyPendingOrScheduledTasks() {
        Task task = task("x", TaskPriority.MEDIUM, 1); task.setStatus(TaskStatus.PENDING);
        when(repository.findById(task.getId())).thenReturn(Optional.of(task));
        assertTrue(service.cancelTask(task.getId()));
        assertEquals(TaskStatus.CANCELLED, task.getStatus());
        task.setStatus(TaskStatus.PROCESSING);
        assertFalse(service.cancelTask(task.getId()));
    }

    @Test void getTaskAndListsMapEntitiesToResponses() {
        Task task = task("x", TaskPriority.MEDIUM, 1);
        when(repository.findById(task.getId())).thenReturn(Optional.of(task));
        assertEquals(task.getId(), service.getTask(task.getId()).orElseThrow().getId());
        Page<Task> page = new PageImpl<>(List.of(task));
        when(repository.findByStatusOrderByPriorityDescCreatedAtAsc(eq(TaskStatus.PENDING), any()))
                .thenReturn(page);
        assertEquals(task.getId(), service.getTasks(TaskStatus.PENDING, PageRequest.of(0, 5)).getContent().get(0).getId());
        when(repository.findAllByOrderByCreatedAtDesc(any())).thenReturn(page);
        assertEquals(task.getId(), service.getTasks(null, PageRequest.of(0, 5)).getContent().get(0).getId());
        when(repository.findByWorkerIdAndStatus("worker-1", TaskStatus.PROCESSING)).thenReturn(List.of(task));
        assertEquals(task.getId(), service.getProcessingTasks().get(0).getId());
    }

    @Test void updatesHeartbeatForConfiguredWorker() {
        service.updateHeartbeat(UUID.randomUUID());
        verify(repository).updateHeartbeat(any(), eq("worker-1"), any());
    }

    @Test void summaryIncludesTypeCountsAndRuntimeEstimate() {
        when(repository.countByTaskTypeAndStatus()).thenReturn(List.<Object[]>of(
                new Object[]{"x", TaskStatus.PENDING, 2L}, new Object[]{"x", TaskStatus.PROCESSING, 1L}));
        when(metrics.getAverageProcessingTimeMs()).thenReturn(1000.0);
        when(metrics.getAverageProcessingTimeMs("x")).thenReturn(1000.0);
        when(metrics.getCurrentPoolSize()).thenReturn(2);
        when(metrics.getConfiguredPoolSize()).thenReturn(2);
        var summary = service.getTaskSummary();
        assertEquals(2L, summary.getPendingTasks());
        assertEquals(1L, summary.getProcessingTasks());
        assertEquals(3L, summary.getTotalTasks());
        assertEquals(java.util.Map.of("x", 2L), summary.getPendingByType());
        assertEquals(2L, summary.getByTaskType().get("x").getPendingTasks());
        assertEquals(1L, summary.getByTaskType().get("x").getProcessingTasks());
        assertEquals(2L, summary.getEstimatedBacklogCompletionSeconds()); // (2+1 outstanding) * 1s / 2 workers
        verify(repository, never()).countByStatus(any());
    }

    private Task task(String type, TaskPriority priority, int maxAttempts) {
        return Task.builder().id(UUID.randomUUID()).taskType(type).priority(priority).maxAttempts(maxAttempts)
                .attemptCount(0).status(TaskStatus.PENDING).build();
    }
}
