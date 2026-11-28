package com.taskprocessor.service;

import com.taskprocessor.entity.Task;
import com.taskprocessor.metrics.TaskMetrics;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.taskprocessor.web.CorrelationIdFilter;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service responsible for processing tasks.
 * Routes tasks to appropriate handlers based on task type.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskProcessorService {

    private final TaskQueueService taskQueueService;
    private final List<TaskHandler> taskHandlers;
    private final TaskMetrics taskMetrics;

    private final Map<String, TaskHandler> handlerMap = new HashMap<>();
    private TaskHandler defaultHandler;

    @PostConstruct
    public void init() {
        // Build handler map
        for (TaskHandler handler : taskHandlers) {
            handlerMap.put(handler.getTaskType(), handler);
            if (handler.getTaskType().equals(DefaultTaskHandler.DEFAULT_TASK_TYPE)) {
                defaultHandler = handler;
            }
        }
        
        log.info("Registered {} task handlers: {}", handlerMap.size(), handlerMap.keySet());
    }

    /**
     * Process a single task
     */
    public void processTask(Task task) {
        long startTime = System.currentTimeMillis();
        // Logging context so every log line from the handler carries the task and the
        // correlation id of the request that created it.
        MDC.put("taskId", task.getId().toString());
        MDC.put("taskType", task.getTaskType());
        MDC.put("attempt", String.valueOf(task.getAttemptCount()));
        if (task.getCorrelationId() != null) {
            MDC.put(CorrelationIdFilter.MDC_KEY, task.getCorrelationId());
        }

        try {
            taskMetrics.incrementActiveProcessing();
            
            // Find appropriate handler
            TaskHandler handler = handlerMap.getOrDefault(task.getTaskType(), defaultHandler);
            
            if (handler == null) {
                throw new IllegalStateException("No handler found for task type: " + task.getTaskType());
            }

            log.debug("Processing task {} with handler {}", task.getId(), handler.getClass().getSimpleName());
            
            // Execute the handler
            handler.handle(task);
            
            // Mark as completed
            taskQueueService.completeTask(task);
            
            long duration = System.currentTimeMillis() - startTime;
            taskMetrics.recordAttempt(task.getTaskType(), "completed", duration);
            log.debug("Task {} completed in {}ms", task.getId(), duration);
            
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Task {} failed after {}ms: {}", task.getId(), duration, e.getMessage(), e);
            
            boolean willRetry = task.getAttemptCount() < (task.getMaxAttempts() == null ? 1 : task.getMaxAttempts());
            taskMetrics.recordAttempt(task.getTaskType(), willRetry ? "retried" : "failed", duration);
            taskQueueService.failTask(task, e.getMessage());
            
        } finally {
            taskMetrics.decrementActiveProcessing();
            MDC.remove("taskId");
            MDC.remove("taskType");
            MDC.remove("attempt");
            MDC.remove(CorrelationIdFilter.MDC_KEY);
        }
    }

    /**
     * Get handler for a specific task type
     */
    public TaskHandler getHandler(String taskType) {
        return handlerMap.getOrDefault(taskType, defaultHandler);
    }

    /**
     * Check if a handler exists for a task type
     */
    public boolean hasHandler(String taskType) {
        return handlerMap.containsKey(taskType) || defaultHandler != null;
    }
}
