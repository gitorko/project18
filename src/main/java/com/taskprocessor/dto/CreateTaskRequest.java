package com.taskprocessor.dto;

import com.taskprocessor.entity.TaskPriority;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateTaskRequest {

    @NotBlank(message = "Task type is required")
    private String taskType;

    @Size(max = 65536, message = "Payload must be at most 65536 characters")
    private String payload;

    @Builder.Default
    private TaskPriority priority = TaskPriority.MEDIUM;

    /**
     * Maximum retry attempts. If not specified, the task is not retried on
     * failure (fails immediately) - retries must be explicitly opted into.
     */
    @Min(value = 1, message = "Max attempts must be at least 1")
    @Max(value = 10, message = "Max attempts must be at most 10")
    private Integer maxAttempts;

    /**
     * Optional scheduled time for delayed execution.
     * If null, task will be processed immediately.
     */
    private Instant scheduledAt;

    /**
     * Max execution time in seconds before the task is cancelled and failed.
     * If null, falls back to the configured default (task-processor.poller.default-task-timeout-seconds).
     */
    @Min(value = 1, message = "Timeout must be at least 1 second")
    private Integer timeoutSeconds;
}
