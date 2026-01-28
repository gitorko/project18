package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.entity.Task;
import com.taskprocessor.metrics.TaskMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background service that polls for tasks and processes them.
 * Uses adaptive polling to reduce database load when queue is empty.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskPollerService {

    private final TaskQueueService taskQueueService;
    private final TaskProcessorService taskProcessorService;
    private final TaskProcessorProperties properties;
    private final ThreadPoolExecutor taskProcessorExecutor;
    private final TaskMetrics taskMetrics;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicLong currentPollInterval = new AtomicLong();
    
    @Getter
    private volatile int currentBatchSize;
    
    private ScheduledExecutorService pollerScheduler;
    private ScheduledExecutorService heartbeatScheduler;
    
    // Track tasks being processed by this worker for heartbeat updates
    private final ConcurrentHashMap<String, Task> processingTasks = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        enabled.set(properties.getPoller().isEnabled());
        currentBatchSize = properties.getPoller().getBatchSize();
        currentPollInterval.set(properties.getPoller().getPollIntervalMs());
        
        if (enabled.get()) {
            start();
        }
        
        log.info("Task poller initialized: workerId={}, enabled={}, poolSize={}, batchSize={}", 
                properties.getWorkerId(),
                enabled.get(),
                properties.getPoller().getPoolSize(),
                currentBatchSize);
    }

    /**
     * Start the poller
     */
    public synchronized void start() {
        if (running.get()) {
            log.warn("Poller is already running");
            return;
        }

        running.set(true);
        
        // Start the polling scheduler
        pollerScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "task-poller");
            t.setDaemon(true);
            return t;
        });
        
        pollerScheduler.scheduleWithFixedDelay(
                this::pollAndProcess,
                0,
                currentPollInterval.get(),
                TimeUnit.MILLISECONDS
        );

        // Start heartbeat scheduler
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "task-heartbeat");
            t.setDaemon(true);
            return t;
        });
        
        heartbeatScheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                properties.getHeartbeat().getIntervalMs(),
                properties.getHeartbeat().getIntervalMs(),
                TimeUnit.MILLISECONDS
        );

        log.info("Task poller started");
    }

    /**
     * Stop the poller gracefully
     */
    @PreDestroy
    public synchronized void stop() {
        if (!running.get()) {
            return;
        }

        running.set(false);
        log.info("Stopping task poller...");

        // Stop accepting new polls
        if (pollerScheduler != null) {
            pollerScheduler.shutdown();
            try {
                if (!pollerScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    pollerScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                pollerScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Stop heartbeats
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdown();
            try {
                if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                heartbeatScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Wait for in-flight tasks to complete
        taskProcessorExecutor.shutdown();
        try {
            if (!taskProcessorExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Some tasks did not complete within timeout, forcing shutdown");
                taskProcessorExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            taskProcessorExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Task poller stopped");
    }

    /**
     * Poll for tasks and submit them for processing
     */
    private void pollAndProcess() {
        if (!enabled.get() || !running.get()) {
            return;
        }

        try {
            // Check if we have capacity to process more tasks
            int availableCapacity = taskProcessorExecutor.getMaximumPoolSize() - 
                    taskProcessorExecutor.getActiveCount();
            
            if (availableCapacity <= 0) {
                log.debug("Thread pool at capacity, skipping poll");
                return;
            }

            int fetchSize = Math.min(currentBatchSize, availableCapacity);
            List<Task> tasks = taskQueueService.dequeueTasks(fetchSize);

            if (tasks.isEmpty()) {
                // Adaptive polling: increase interval when queue is empty
                if (properties.getPoller().isAdaptivePolling()) {
                    long newInterval = Math.min(
                            currentPollInterval.get() * 2,
                            properties.getPoller().getMaxPollIntervalMs()
                    );
                    currentPollInterval.set(newInterval);
                }
                return;
            }

            // Reset poll interval when tasks are found
            if (properties.getPoller().isAdaptivePolling()) {
                currentPollInterval.set(properties.getPoller().getMinPollIntervalMs());
            }

            // Submit tasks for processing
            for (Task task : tasks) {
                processingTasks.put(task.getId().toString(), task);
                
                CompletableFuture.runAsync(() -> {
                    try {
                        taskProcessorService.processTask(task);
                    } finally {
                        processingTasks.remove(task.getId().toString());
                    }
                }, taskProcessorExecutor).exceptionally(ex -> {
                    log.error("Unexpected error processing task {}: {}", task.getId(), ex.getMessage());
                    processingTasks.remove(task.getId().toString());
                    return null;
                });
            }

            taskMetrics.recordTasksPolled(tasks.size());
            log.debug("Submitted {} tasks for processing", tasks.size());

        } catch (Exception e) {
            log.error("Error during poll cycle: {}", e.getMessage(), e);
        }
    }

    /**
     * Send heartbeats for all tasks being processed by this worker
     */
    private void sendHeartbeats() {
        if (!running.get()) {
            return;
        }

        try {
            for (Map.Entry<String, Task> entry : processingTasks.entrySet()) {
                taskQueueService.updateHeartbeat(entry.getValue().getId());
            }
            
            if (!processingTasks.isEmpty()) {
                log.debug("Sent heartbeats for {} tasks", processingTasks.size());
            }
        } catch (Exception e) {
            log.error("Error sending heartbeats: {}", e.getMessage(), e);
        }
    }

    /**
     * Enable or disable the poller
     */
    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
        log.info("Poller enabled: {}", enabled);
    }

    /**
     * Check if poller is enabled
     */
    public boolean isEnabled() {
        return enabled.get();
    }

    /**
     * Check if poller is running
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Update the batch size
     */
    public void setBatchSize(int batchSize) {
        this.currentBatchSize = batchSize;
        log.info("Batch size updated to: {}", batchSize);
    }

    /**
     * Update the poll interval
     */
    public void setPollInterval(long intervalMs) {
        this.currentPollInterval.set(intervalMs);
        log.info("Poll interval updated to: {}ms", intervalMs);
    }

    /**
     * Update the thread pool size
     */
    public void setPoolSize(int poolSize) {
        int currentCore = taskProcessorExecutor.getCorePoolSize();
        int currentMax = taskProcessorExecutor.getMaximumPoolSize();
        
        // Ensure we don't violate core <= max constraint
        if (poolSize > currentMax) {
            taskProcessorExecutor.setMaximumPoolSize(poolSize);
            taskProcessorExecutor.setCorePoolSize(poolSize);
        } else {
            taskProcessorExecutor.setCorePoolSize(poolSize);
            taskProcessorExecutor.setMaximumPoolSize(Math.max(poolSize, currentMax));
        }
        
        log.info("Thread pool size updated: core={}, max={}", 
                taskProcessorExecutor.getCorePoolSize(),
                taskProcessorExecutor.getMaximumPoolSize());
    }

    /**
     * Get current poll interval
     */
    public long getCurrentPollInterval() {
        return currentPollInterval.get();
    }

    /**
     * Get number of tasks currently being processed
     */
    public int getProcessingTaskCount() {
        return processingTasks.size();
    }
}
