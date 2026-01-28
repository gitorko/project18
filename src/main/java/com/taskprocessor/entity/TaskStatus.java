package com.taskprocessor.entity;

/**
 * Possible states of a task in the queue.
 */
public enum TaskStatus {
    /**
     * Task is waiting to be processed
     */
    PENDING,
    
    /**
     * Task is currently being processed by a worker
     */
    PROCESSING,
    
    /**
     * Task completed successfully
     */
    COMPLETED,
    
    /**
     * Task failed after all retry attempts
     */
    FAILED,
    
    /**
     * Task was manually cancelled
     */
    CANCELLED,
    
    /**
     * Task is scheduled for future execution
     */
    SCHEDULED
}
