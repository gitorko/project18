package com.taskprocessor.service;

import com.taskprocessor.entity.Task;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Default task handler that processes tasks with no specific handler.
 * This is a fallback handler that simply logs the task processing.
 * 
 * In a real application, you would create specific handlers for each task type.
 */
@Slf4j
@Component
public class DefaultTaskHandler implements TaskHandler {

    public static final String DEFAULT_TASK_TYPE = "default";

    @Override
    public String getTaskType() {
        return DEFAULT_TASK_TYPE;
    }

    @Override
    public void handle(Task task) throws Exception {
        log.info("Processing task: id={}, type={}, payload={}", 
                task.getId(), task.getTaskType(), task.getPayload());
        
        // Simulate some work
        Thread.sleep(100);
        
        log.info("Completed task: id={}", task.getId());
    }

    @Override
    public long getEstimatedDurationMs() {
        return 100;
    }
}
