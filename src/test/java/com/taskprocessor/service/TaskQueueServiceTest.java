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

    @Test void dequeueMarksSelectedRowsAndRoundRobinsWithinPriority() {
        Task a1 = task("a", TaskPriority.MEDIUM, 10), a2 = task("a", TaskPriority.MEDIUM, 10),
                b1 = task("b", TaskPriority.MEDIUM, 10), low = task("low", TaskPriority.LOW, 10);
        when(repository.findTasksForProcessing(any(), anyInt())).thenReturn(List.of(a1, a2, b1, low));
        when(repository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));
        List<Task> result = service.dequeueTasks(3);
        assertEquals(List.of(a1, b1, a2), result);
        assertTrue(result.stream().allMatch(t -> t.getStatus() == TaskStatus.PROCESSING));
        assertTrue(result.stream().allMatch(t -> "worker-1".equals(t.getWorkerId())));
        assertTrue(result.stream().allMatch(t -> t.getAttemptCount() == 1));
        verify(repository).findTasksForProcessing(any(), eq(15));
    }

    @Test void dequeueReturnsEmptyWhenNoCandidates() {
        when(repository.findTasksForProcessing(any(), anyInt())).thenReturn(List.of());
        assertTrue(service.dequeueTasks(4).isEmpty());
        verify(repository, never()).saveAll(anyList());
    }

    @Test void typeCapacitySkipsCappedTypeAndUsesOtherAvailableTypes() {
        Task capped = task("heavy", TaskPriority.HIGH, 1);
        Task other = task("fast", TaskPriority.HIGH, 1);
        when(repository.findTasksForProcessingExcludingTypes(any(), anyInt(), eq(java.util.Set.of("heavy"))))
                .thenReturn(List.of(other));
        when(repository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        List<Task> selected = service.dequeueTasks(1, java.util.Map.of("heavy", 0));

        assertEquals(List.of(other), selected);
        assertEquals(TaskStatus.PENDING, capped.getStatus());
        verify(repository, never()).findTasksForProcessing(any(), anyInt());
    }

    @Test void saturatedTypeCannotCrowdOutOtherTypesFromTheCandidatePool() {
        // batch 3 -> pool of 15, entirely filled by the capped type
        List<Task> heavy = java.util.stream.IntStream.range(0, 15)
                .mapToObj(i -> task("heavy", TaskPriority.HIGH, 1)).toList();
        Task fast1 = task("fast", TaskPriority.MEDIUM, 1), fast2 = task("fast", TaskPriority.MEDIUM, 1);
        when(repository.findTasksForProcessing(any(), anyInt())).thenReturn(heavy);
        when(repository.findTasksForProcessingExcludingTypes(any(), anyInt(), eq(java.util.Set.of("heavy"))))
                .thenReturn(List.of(fast1, fast2));
        when(repository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

        List<Task> selected = service.dequeueTasks(3, java.util.Map.of("heavy", 1));

        assertEquals(List.of(heavy.get(0), fast1, fast2), selected);
    }

    @Test void refetchSkipsRowsAlreadyConsideredInAnEarlierPass() {
        List<Task> heavy = java.util.stream.IntStream.range(0, 15)
                .mapToObj(i -> task("heavy", TaskPriority.HIGH, 1)).toList();
        Task fast = task("fast", TaskPriority.MEDIUM, 1);
        when(repository.findTasksForProcessing(any(), anyInt())).thenReturn(heavy);
        when(repository.findTasksForProcessingExcludingTypes(any(), anyInt(), any()))
                .thenReturn(List.of(heavy.get(0), fast));
        when(repository.saveAll(anyList())).thenAnswer(inv -> inv.getArgument(0));

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

    @Test void completeTaskUpdatesOnlyProcessingTasks() {
        Task task = task("x", TaskPriority.MEDIUM, 1);
        task.setStatus(TaskStatus.PROCESSING); task.setStartedAt(Instant.now().minusMillis(50));
        when(repository.findByIdForUpdate(task.getId())).thenReturn(Optional.of(task));
        service.completeTask(task.getId(), task.getAttemptCount());
        assertEquals(TaskStatus.COMPLETED, task.getStatus());
        assertNotNull(task.getCompletedAt());
        verify(repository).save(task);
        verify(metrics).recordTaskCompleted(eq("x"), longThat(duration -> duration >= 0));

        task.setStatus(TaskStatus.FAILED);
        clearInvocations(repository, metrics);
        service.completeTask(task.getId(), task.getAttemptCount());
        verify(repository, never()).save(any());
        verifyNoInteractions(metrics);
    }

    @Test void staleAttemptCannotCompleteNewerRetry() {
        Task currentAttempt = task("x", TaskPriority.MEDIUM, 3);
        currentAttempt.setStatus(TaskStatus.PROCESSING);
        currentAttempt.setAttemptCount(2);
        when(repository.findByIdForUpdate(currentAttempt.getId())).thenReturn(Optional.of(currentAttempt));
        service.completeTask(currentAttempt.getId(), 1);
        verify(repository, never()).save(any());
        verifyNoInteractions(metrics);
    }

    @Test void failTaskRetriesThenMarksExhaustedTaskFailed() {
        Task task = task("x", TaskPriority.MEDIUM, 2); task.setStatus(TaskStatus.PROCESSING); task.setAttemptCount(1);
        when(repository.findByIdForUpdate(task.getId())).thenReturn(Optional.of(task));
        Instant beforeRetry = Instant.now();
        service.failTask(task.getId(), task.getAttemptCount(), "retry me");
        assertEquals(TaskStatus.SCHEDULED, task.getStatus());
        assertTrue(task.getScheduledAt().isAfter(beforeRetry));
        assertTrue(task.getScheduledAt().isBefore(beforeRetry.plusSeconds(2)));
        assertNull(task.getWorkerId());
        assertEquals("retry me", task.getErrorMessage());

        task.setStatus(TaskStatus.PROCESSING); task.setAttemptCount(2);
        service.failTask(task.getId(), task.getAttemptCount(), "terminal");
        assertEquals(TaskStatus.FAILED, task.getStatus());
        assertNotNull(task.getCompletedAt());
        verify(metrics).recordTaskFailed("x");
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
