package com.taskprocessor.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
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

    private String payload;

    @Min(value = -100, message = "Priority must be at least -100")
    @Max(value = 100, message = "Priority must be at most 100")
    @Builder.Default
    private Integer priority = 0;

    @Min(value = 1, message = "Max attempts must be at least 1")
    @Max(value = 10, message = "Max attempts must be at most 10")
    @Builder.Default
    private Integer maxAttempts = 3;

    /**
     * Optional scheduled time for delayed execution.
     * If null, task will be processed immediately.
     */
    private Instant scheduledAt;
}
