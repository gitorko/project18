package com.taskprocessor.dto;

import com.taskprocessor.entity.TaskPriority;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateTaskScheduleRequest {

    @NotBlank(message = "Task type is required")
    private String taskType;

    private String payload;

    @Builder.Default
    private TaskPriority priority = TaskPriority.MEDIUM;

    /** First run time; if omitted or in the past, the schedule starts now. */
    private Instant scheduledAt;

    @NotNull(message = "Interval is required")
    @Min(value = 1, message = "Interval must be at least 1 second")
    @Max(value = 31536000, message = "Interval cannot exceed one year")
    private Integer intervalSeconds;

    @Min(value = 1, message = "Max attempts must be at least 1")
    @Max(value = 10, message = "Max attempts must be at most 10")
    private Integer maxAttempts;

    @Min(value = 1, message = "Timeout must be at least 1 second")
    private Integer timeoutSeconds;
}
