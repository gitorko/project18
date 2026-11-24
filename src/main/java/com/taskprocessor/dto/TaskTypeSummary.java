package com.taskprocessor.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Queue and execution counts for one registered task type. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskTypeSummary {
    private long pendingTasks;
    private long scheduledTasks;
    private long processingTasks;
    private long completedTasks;
    private long failedTasks;
    private long cancelledTasks;
    private double avgProcessingTimeMs;
}
