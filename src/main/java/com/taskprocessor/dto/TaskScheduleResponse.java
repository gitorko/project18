package com.taskprocessor.dto;

import com.taskprocessor.entity.TaskPriority;
import com.taskprocessor.entity.TaskSchedule;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.UUID;

@Value
@Builder
public class TaskScheduleResponse {
    UUID id;
    String taskType;
    TaskPriority priority;
    Integer maxAttempts;
    Integer timeoutSeconds;
    Integer intervalSeconds;
    Instant nextRunAt;
    UUID activeTaskId;
    boolean enabled;

    public static TaskScheduleResponse fromEntity(TaskSchedule schedule) {
        return TaskScheduleResponse.builder()
                .id(schedule.getId())
                .taskType(schedule.getTaskType())
                .priority(schedule.getPriority())
                .maxAttempts(schedule.getMaxAttempts())
                .timeoutSeconds(schedule.getTimeoutSeconds())
                .intervalSeconds(schedule.getIntervalSeconds())
                .nextRunAt(schedule.getNextRunAt())
                .activeTaskId(schedule.getActiveTaskId())
                .enabled(schedule.isEnabled())
                .build();
    }
}
