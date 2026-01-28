package com.taskprocessor.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PollerConfigRequest {

    @Min(value = 1, message = "Pool size must be at least 1")
    @Max(value = 100, message = "Pool size must be at most 100")
    private Integer poolSize;

    @Min(value = 1, message = "Batch size must be at least 1")
    @Max(value = 100, message = "Batch size must be at most 100")
    private Integer batchSize;

    @Min(value = 100, message = "Poll interval must be at least 100ms")
    @Max(value = 60000, message = "Poll interval must be at most 60000ms")
    private Long pollIntervalMs;

    private Boolean enabled;
}
