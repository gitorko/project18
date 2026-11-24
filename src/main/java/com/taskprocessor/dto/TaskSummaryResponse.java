package com.taskprocessor.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskSummaryResponse {

    private long totalTasks;
    private long pendingTasks;
    private long processingTasks;
    private long completedTasks;
    private long failedTasks;
    private long cancelledTasks;
    private long scheduledTasks;
    
    /**
     * Distribution of pending tasks by type
     */
    private Map<String, Long> pendingByType;

    /** Counts and observed runtime, grouped by task type. */
    private Map<String, TaskTypeSummary> byTaskType;
    
    /**
     * Current processing rate (tasks per minute)
     */
    private double processingRatePerMinute;
    
    /**
     * Average processing time in milliseconds
     */
    private double avgProcessingTimeMs;
    
    /**
     * Number of active workers
     */
    private int activeWorkers;
    
    /**
     * Current thread pool size
     */
    private int currentPoolSize;

    /** Estimated seconds to drain pending and running work; null until runtime samples exist. */
    private Long estimatedBacklogCompletionSeconds;
}
