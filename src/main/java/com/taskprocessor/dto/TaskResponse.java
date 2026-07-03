package com.taskprocessor.dto;

import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskResponse {

    private UUID id;
    private String taskType;
    private String payload;
    private TaskStatus status;
    private Integer priority;
    private Integer attemptCount;
    private Integer maxAttempts;
    private String workerId;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant startedAt;
    private Instant completedAt;
    private Instant scheduledAt;
    private String errorMessage;
    private Integer timeoutSeconds;

    public static TaskResponse fromEntity(Task task) {
        return TaskResponse.builder()
                .id(task.getId())
                .taskType(task.getTaskType())
                .payload(task.getPayload())
                .status(task.getStatus())
                .priority(task.getPriority())
                .attemptCount(task.getAttemptCount())
                .maxAttempts(task.getMaxAttempts())
                .workerId(task.getWorkerId())
                .createdAt(task.getCreatedAt())
                .updatedAt(task.getUpdatedAt())
                .startedAt(task.getStartedAt())
                .completedAt(task.getCompletedAt())
                .scheduledAt(task.getScheduledAt())
                .errorMessage(task.getErrorMessage())
                .timeoutSeconds(task.getTimeoutSeconds())
                .build();
    }
}
