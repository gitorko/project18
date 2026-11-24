package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.CreateTaskRequest;
import com.taskprocessor.dto.TaskResponse;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskPriority;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.metrics.TaskMetrics;
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
    private TaskProcessorProperties properties;
    private TaskQueueService service;

    @BeforeEach void setUp() {
        properties = new TaskProcessorProperties();
        properties.setWorkerId("worker-1");
        service = new TaskQueueService(repository, properties, metrics);
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
        service.failTask(task.getId(), task.getAttemptCount(), "retry me");
        assertEquals(TaskStatus.PENDING, task.getStatus());
        assertNull(task.getWorkerId());
        assertEquals("retry me", task.getErrorMessage());

        task.setStatus(TaskStatus.PROCESSING); task.setAttemptCount(2);
        service.failTask(task.getId(), task.getAttemptCount(), "terminal");
        assertEquals(TaskStatus.FAILED, task.getStatus());
        assertNotNull(task.getCompletedAt());
        verify(metrics).recordTaskFailed("x");
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
        when(repository.countByStatus(any())).thenReturn(2L);
        when(repository.countByTaskTypeAndStatus(TaskStatus.PENDING)).thenReturn(List.<Object[]>of(new Object[]{"x", 2L}));
        when(repository.countByTaskTypeAndStatus()).thenReturn(List.<Object[]>of(
                new Object[]{"x", TaskStatus.PENDING, 2L}, new Object[]{"x", TaskStatus.PROCESSING, 1L}));
        when(metrics.getAverageProcessingTimeMs()).thenReturn(1000.0);
        when(metrics.getAverageProcessingTimeMs("x")).thenReturn(1000.0);
        when(metrics.getCurrentPoolSize()).thenReturn(2);
        when(metrics.getConfiguredPoolSize()).thenReturn(2);
        var summary = service.getTaskSummary();
        assertEquals(2L, summary.getPendingTasks());
        assertEquals(2L, summary.getByTaskType().get("x").getPendingTasks());
        assertEquals(1L, summary.getByTaskType().get("x").getProcessingTasks());
        assertEquals(2L, summary.getEstimatedBacklogCompletionSeconds());
    }

    private Task task(String type, TaskPriority priority, int maxAttempts) {
        return Task.builder().id(UUID.randomUUID()).taskType(type).priority(priority).maxAttempts(maxAttempts)
                .attemptCount(0).status(TaskStatus.PENDING).build();
    }
}
