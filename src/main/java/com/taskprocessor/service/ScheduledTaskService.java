package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.CreateTaskScheduleRequest;
import com.taskprocessor.dto.TaskScheduleResponse;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskPriority;
import com.taskprocessor.entity.TaskSchedule;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.metrics.TaskMetrics;
import com.taskprocessor.repository.TaskRepository;
import com.taskprocessor.repository.TaskScheduleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class ScheduledTaskService {

    private static final int DISPATCH_BATCH_SIZE = 100;

    private final TaskScheduleRepository scheduleRepository;
    private final TaskRepository taskRepository;
    private final TaskProcessorProperties properties;
    private final TaskMetrics taskMetrics;
    private final QueueAdmissionService queueAdmissionService;

    @Transactional
    public TaskScheduleResponse createSchedule(CreateTaskScheduleRequest request) {
        Instant now = Instant.now();
        Instant firstRunAt = request.getScheduledAt() == null || request.getScheduledAt().isBefore(now)
                ? now : request.getScheduledAt();

        TaskSchedule schedule = TaskSchedule.builder()
                .taskType(request.getTaskType())
                .payload(request.getPayload())
                .priority(request.getPriority() == null ? TaskPriority.MEDIUM : request.getPriority())
                .maxAttempts(request.getMaxAttempts() == null ? 1 : request.getMaxAttempts())
                .timeoutSeconds(request.getTimeoutSeconds() == null
                        ? properties.getPoller().getDefaultTaskTimeoutSeconds()
                        : request.getTimeoutSeconds())
                .intervalSeconds(request.getIntervalSeconds())
                .nextRunAt(firstRunAt)
                .enabled(true)
                .build();

        return TaskScheduleResponse.fromEntity(scheduleRepository.save(schedule));
    }

    @Transactional(readOnly = true)
    public Optional<TaskScheduleResponse> getSchedule(UUID scheduleId) {
        return scheduleRepository.findById(scheduleId).map(TaskScheduleResponse::fromEntity);
    }

    @Transactional
    public boolean cancelSchedule(UUID scheduleId) {
        return scheduleRepository.findByIdForUpdate(scheduleId).map(schedule -> {
            schedule.setEnabled(false);
            schedule.setNextRunAt(null);
            if (schedule.getActiveTaskId() != null) {
                taskRepository.findByIdForUpdate(schedule.getActiveTaskId()).ifPresent(task -> {
                    if (task.getStatus() == TaskStatus.PENDING) {
                        task.setStatus(TaskStatus.CANCELLED);
                        task.setCompletedAt(Instant.now());
                        taskRepository.save(task);
                        schedule.setActiveTaskId(null);
                    }
                    // A running handler is allowed to finish, but the disabled schedule
                    // will not create another occurrence when it does.
                });
            }
            scheduleRepository.save(schedule);
            return true;
        }).orElse(false);
    }

    /**
     * Promote due one-time tasks and materialize recurring occurrences. The row locks
     * let multiple application instances run this dispatcher without duplicate runs.
     */
    @Scheduled(fixedDelayString = "${task-processor.scheduler.poll-interval-ms:1000}")
    @Transactional
    public void dispatchDueTasks() {
        Instant now = Instant.now();
        scheduleRepository.releaseTerminalOccurrences(now);
        int promoted = taskRepository.promoteDueScheduledTasks(now, DISPATCH_BATCH_SIZE);
        if (promoted > 0) {
            log.debug("Promoted {} due one-time tasks", promoted);
        }

        List<TaskSchedule> dueSchedules = scheduleRepository.findDueSchedulesForUpdate(now, DISPATCH_BATCH_SIZE);
        for (TaskSchedule schedule : dueSchedules) {
            Task occurrence = Task.builder()
                    .taskType(schedule.getTaskType())
                    .payload(schedule.getPayload())
                    .priority(schedule.getPriority())
                    .maxAttempts(schedule.getMaxAttempts())
                    .timeoutSeconds(schedule.getTimeoutSeconds())
                    .status(TaskStatus.PENDING)
                    .scheduledAt(schedule.getNextRunAt())
                    .scheduleId(schedule.getId())
                    .build();
            if (!queueAdmissionService.canAccept(List.of(occurrence))) {
                log.debug("Queue capacity reached; leaving recurring schedule {} due", schedule.getId());
                continue;
            }
            occurrence = taskRepository.saveAndFlush(occurrence);

            schedule.setActiveTaskId(occurrence.getId());
            schedule.setNextRunAt(null);
            scheduleRepository.save(schedule);
            taskMetrics.recordTaskEnqueued(occurrence.getTaskType());
        }
        if (!dueSchedules.isEmpty()) {
            log.info("Created {} recurring task occurrences", dueSchedules.size());
        }
    }
}
