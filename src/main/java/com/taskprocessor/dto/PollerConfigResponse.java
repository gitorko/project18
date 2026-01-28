package com.taskprocessor.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PollerConfigResponse {

    private int poolSize;
    private int batchSize;
    private long pollIntervalMs;
    private boolean enabled;
    private int activeThreads;
    private long heartbeatIntervalMs;
    private long staleTaskThresholdMs;
    private String workerId;
}
