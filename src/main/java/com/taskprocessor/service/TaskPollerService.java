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

    // Track tasks being processed by this worker for heartbeat updates and timeout enforcement
    private final ConcurrentHashMap<String, ProcessingHandle> processingTasks = new ConcurrentHashMap<>();

    /**
     * Bundles a task with the Future controlling its execution, so the timeout
     * watchdog can cancel (best-effort interrupt) a task that overran its deadline.
     */
    private record ProcessingHandle(Task task, Future<?> future) {
    }

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

            // Submit tasks for processing. We use a plain Future (not
            // CompletableFuture.runAsync) so the timeout watchdog can cancel/interrupt
            // a task that overruns its deadline.
            for (Task task : tasks) {
                String taskKey = task.getId().toString();

                Future<?> future = taskProcessorExecutor.submit(() -> {
                    try {
                        taskProcessorService.processTask(task);
                    } catch (Exception ex) {
                        log.error("Unexpected error processing task {}: {}", task.getId(), ex.getMessage());
                    } finally {
                        processingTasks.remove(taskKey);
                    }
                });

                processingTasks.put(taskKey, new ProcessingHandle(task, future));
            }

            taskMetrics.recordTasksPolled(tasks.size());
            log.debug("Submitted {} tasks for processing", tasks.size());

        } catch (Exception e) {
            log.error("Error during poll cycle: {}", e.getMessage(), e);
        }
    }

    /**
     * Send heartbeats for all tasks being processed by this worker, and check for
     * tasks that have overrun their execution timeout.
     */
    private void sendHeartbeats() {
        if (!running.get()) {
            return;
        }

        try {
            for (Map.Entry<String, ProcessingHandle> entry : processingTasks.entrySet()) {
                Task task = entry.getValue().task();

                if (hasTimedOut(task)) {
                    // Atomically claim this task so a concurrent normal completion
                    // doesn't also try to handle it (see TaskQueueService's
                    // PROCESSING-status guard for the corresponding DB-side safeguard).
                    ProcessingHandle claimed = processingTasks.remove(entry.getKey());
                    if (claimed != null) {
                        int timeoutSeconds = effectiveTimeoutSeconds(task);
                        log.warn("Task {} exceeded timeout of {}s, cancelling", task.getId(), timeoutSeconds);
                        claimed.future().cancel(true);
                        taskQueueService.failTask(task.getId(),
                                "Task exceeded timeout of " + timeoutSeconds + "s");
                    }
                    continue;
                }

                taskQueueService.updateHeartbeat(task.getId());
            }

            if (!processingTasks.isEmpty()) {
                log.debug("Sent heartbeats for {} tasks", processingTasks.size());
            }
        } catch (Exception e) {
            log.error("Error sending heartbeats: {}", e.getMessage(), e);
        }
    }

    private boolean hasTimedOut(Task task) {
        long elapsedMs = System.currentTimeMillis() - task.getStartedAt().toEpochMilli();
        long timeoutMs = effectiveTimeoutSeconds(task) * 1000L;
        return elapsedMs > timeoutMs;
    }

    private int effectiveTimeoutSeconds(Task task) {
        return task.getTimeoutSeconds() != null
                ? task.getTimeoutSeconds()
                : properties.getPoller().getDefaultTaskTimeoutSeconds();
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
