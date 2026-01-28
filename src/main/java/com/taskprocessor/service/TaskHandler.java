package com.taskprocessor.service;

import com.taskprocessor.entity.Task;

/**
 * Interface for task handlers.
 * Implement this interface to define how specific task types should be processed.
 */
public interface TaskHandler {

    /**
     * Returns the task type this handler can process.
     */
    String getTaskType();

    /**
     * Process the task.
     * 
     * @param task The task to process
     * @throws Exception if processing fails (will trigger retry if attempts remain)
     */
    void handle(Task task) throws Exception;

    /**
     * Returns the estimated duration of this task type in milliseconds.
     * Used for metrics and scheduling decisions.
     */
    default long getEstimatedDurationMs() {
        return 1000;
    }
}
