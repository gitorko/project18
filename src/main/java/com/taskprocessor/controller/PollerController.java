package com.taskprocessor.controller;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.PollerConfigRequest;
import com.taskprocessor.dto.PollerConfigResponse;
import com.taskprocessor.service.TaskPollerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * REST API for managing the task poller configuration at runtime.
 * Allows dynamic adjustment of processing capacity.
 */
@RestController
@RequestMapping("/api/v1/poller")
@RequiredArgsConstructor
public class PollerController {

    private final TaskPollerService pollerService;
    private final TaskProcessorProperties properties;
    private final ThreadPoolExecutor taskProcessorExecutor;

    /**
     * Get current poller configuration
     */
    @GetMapping("/config")
    public ResponseEntity<PollerConfigResponse> getConfig() {
        PollerConfigResponse config = PollerConfigResponse.builder()
                .poolSize(taskProcessorExecutor.getCorePoolSize())
                .batchSize(pollerService.getCurrentBatchSize())
                .pollIntervalMs(pollerService.getCurrentPollInterval())
                .enabled(pollerService.isEnabled())
                .activeThreads(taskProcessorExecutor.getActiveCount())
                .heartbeatIntervalMs(properties.getHeartbeat().getIntervalMs())
                .staleTaskThresholdMs(properties.getHeartbeat().getStaleThresholdMs())
                .workerId(properties.getWorkerId())
                .build();
        
        return ResponseEntity.ok(config);
    }

    /**
     * Update poller configuration dynamically.
     * This allows scaling the processing capacity at runtime.
     */
    @PutMapping("/config")
    public ResponseEntity<PollerConfigResponse> updateConfig(@Valid @RequestBody PollerConfigRequest request) {
        if (request.getPoolSize() != null) {
            pollerService.setPoolSize(request.getPoolSize());
        }
        
        if (request.getBatchSize() != null) {
            pollerService.setBatchSize(request.getBatchSize());
        }
        
        if (request.getPollIntervalMs() != null) {
            pollerService.setPollInterval(request.getPollIntervalMs());
        }
        
        if (request.getEnabled() != null) {
            pollerService.setEnabled(request.getEnabled());
        }
        
        return getConfig();
    }

    /**
     * Start the poller if stopped
     */
    @PostMapping("/start")
    public ResponseEntity<String> startPoller() {
        if (pollerService.isRunning()) {
            return ResponseEntity.ok("Poller is already running");
        }
        pollerService.start();
        return ResponseEntity.ok("Poller started");
    }

    /**
     * Stop the poller gracefully
     */
    @PostMapping("/stop")
    public ResponseEntity<String> stopPoller() {
        if (!pollerService.isRunning()) {
            return ResponseEntity.ok("Poller is already stopped");
        }
        pollerService.stop();
        return ResponseEntity.ok("Poller stopped");
    }

    /**
     * Get poller status
     */
    @GetMapping("/status")
    public ResponseEntity<PollerStatusResponse> getStatus() {
        return ResponseEntity.ok(PollerStatusResponse.builder()
                .running(pollerService.isRunning())
                .enabled(pollerService.isEnabled())
                .processingTaskCount(pollerService.getProcessingTaskCount())
                .activeThreads(taskProcessorExecutor.getActiveCount())
                .poolSize(taskProcessorExecutor.getPoolSize())
                .maxPoolSize(taskProcessorExecutor.getMaximumPoolSize())
                .queuedTasks(taskProcessorExecutor.getQueue().size())
                .completedTasks(taskProcessorExecutor.getCompletedTaskCount())
                .workerId(properties.getWorkerId())
                .build());
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class PollerStatusResponse {
        private boolean running;
        private boolean enabled;
        private int processingTaskCount;
        private int activeThreads;
        private int poolSize;
        private int maxPoolSize;
        private int queuedTasks;
        private long completedTasks;
        private String workerId;
    }
}
