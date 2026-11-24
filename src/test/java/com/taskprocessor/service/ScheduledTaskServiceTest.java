package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.CreateTaskScheduleRequest;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskPriority;
import com.taskprocessor.entity.TaskSchedule;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.metrics.TaskMetrics;
import com.taskprocessor.repository.TaskRepository;
import com.taskprocessor.repository.TaskScheduleRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ScheduledTaskServiceTest {
    private TaskScheduleRepository scheduleRepository;
    private TaskRepository taskRepository;
    private TaskMetrics metrics;
    private QueueAdmissionService admissionService;
    private ScheduledTaskService service;

    @BeforeEach void setUp() {
        scheduleRepository = mock(TaskScheduleRepository.class);
        taskRepository = mock(TaskRepository.class);
        metrics = mock(TaskMetrics.class);
        admissionService = mock(QueueAdmissionService.class);
        service = new ScheduledTaskService(scheduleRepository, taskRepository,
                new TaskProcessorProperties(), metrics, admissionService);
    }

    @Test void createsIntervalScheduleWithDefaultsAndRequestedFirstRun() {
        Instant firstRun = Instant.now().plusSeconds(60);
        when(scheduleRepository.save(any(TaskSchedule.class))).thenAnswer(inv -> inv.getArgument(0));

        var response = service.createSchedule(CreateTaskScheduleRequest.builder()
                .taskType("email").intervalSeconds(300).scheduledAt(firstRun).build());

        assertEquals("email", response.getTaskType());
        assertEquals(TaskPriority.MEDIUM, response.getPriority());
        assertEquals(300, response.getIntervalSeconds());
        assertEquals(1, response.getMaxAttempts());
        assertEquals(firstRun, response.getNextRunAt());
        assertTrue(response.isEnabled());
    }

    @Test void dispatchMaterializesRecurringRunAndMarksScheduleActive() {
        Instant dueAt = Instant.now().minusSeconds(2);
        UUID scheduleId = UUID.randomUUID();
        TaskSchedule schedule = TaskSchedule.builder().id(scheduleId).taskType("email").payload("{}")
                .priority(TaskPriority.HIGH).intervalSeconds(60).maxAttempts(2).nextRunAt(dueAt).enabled(true).build();
        when(scheduleRepository.findDueSchedulesForUpdate(any(), eq(100))).thenReturn(java.util.List.of(schedule));
        when(admissionService.canAccept(any())).thenReturn(true);
        when(taskRepository.saveAndFlush(any(Task.class))).thenAnswer(inv -> {
            Task task = inv.getArgument(0);
            task.setId(UUID.randomUUID());
            return task;
        });

        service.dispatchDueTasks();

        assertNotNull(schedule.getActiveTaskId());
        assertNull(schedule.getNextRunAt());
        verify(taskRepository).promoteDueScheduledTasks(any(), eq(100));
        verify(taskRepository).saveAndFlush(argThat(task -> task.getStatus() == TaskStatus.PENDING
                && task.getScheduleId().equals(scheduleId)
                && task.getScheduledAt().equals(dueAt)
                && task.getPriority() == TaskPriority.HIGH));
        verify(metrics).recordTaskEnqueued("email");
        verify(scheduleRepository).releaseTerminalOccurrences(any());
    }

    @Test void cancellationStopsScheduleAndCancelsAnUnstartedOccurrence() {
        UUID scheduleId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        TaskSchedule schedule = TaskSchedule.builder().id(scheduleId).enabled(true).activeTaskId(taskId).build();
        Task task = Task.builder().id(taskId).status(TaskStatus.PENDING).build();
        when(scheduleRepository.findByIdForUpdate(scheduleId)).thenReturn(Optional.of(schedule));
        when(taskRepository.findByIdForUpdate(taskId)).thenReturn(Optional.of(task));

        assertTrue(service.cancelSchedule(scheduleId));

        assertFalse(schedule.isEnabled());
        assertNull(schedule.getNextRunAt());
        assertNull(schedule.getActiveTaskId());
        assertEquals(TaskStatus.CANCELLED, task.getStatus());
        verify(taskRepository).save(task);
        verify(scheduleRepository).save(schedule);
    }

    @Test void returnsFalseWhenCancellingUnknownSchedule() {
        UUID id = UUID.randomUUID();
        when(scheduleRepository.findByIdForUpdate(id)).thenReturn(Optional.empty());
        assertFalse(service.cancelSchedule(id));
    }
}
