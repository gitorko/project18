package com.taskprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskprocessor.entity.Task;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Sample task handler demonstrating how to implement custom task processing.
 * 
 * To add new task types:
 * 1. Create a new class implementing TaskHandler
 * 2. Return your task type name from getTaskType()
 * 3. Implement your processing logic in handle()
 * 
 * The handler will be automatically registered and used for matching task types.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SampleTaskHandler implements TaskHandler {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String getTaskType() {
        return "sample";
    }

    @Override
    public void handle(Task task) throws Exception {
        log.info("Processing sample task: id={}", task.getId());
        
        // Parse payload if present
        if (task.getPayload() != null && !task.getPayload().isEmpty()) {
            try {
                JsonNode payload = objectMapper.readTree(task.getPayload());
                log.info("Task payload: {}", payload);
                
                // Simulate processing based on payload
                int processingTimeMs = payload.has("processingTimeMs") 
                        ? payload.get("processingTimeMs").asInt() 
                        : 500;
                
                // Simulate work
                Thread.sleep(processingTimeMs);
                
                // Simulate random failures for testing retry logic
                if (payload.has("failRate")) {
                    double failRate = payload.get("failRate").asDouble();
                    if (Math.random() < failRate) {
                        throw new RuntimeException("Simulated failure for testing");
                    }
                }
            } catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw e;
                }
                log.warn("Failed to parse payload: {}", e.getMessage());
            }
        } else {
            // Default processing time
            Thread.sleep(500);
        }
        
        log.info("Completed sample task: id={}", task.getId());
    }

    @Override
    public long getEstimatedDurationMs() {
        return 500;
    }
}
