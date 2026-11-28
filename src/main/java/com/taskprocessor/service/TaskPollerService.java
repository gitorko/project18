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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    // Poll scheduling state, guarded by pollLock. A task finishing frees a worker slot, so it
    // can request an early poll instead of letting the slot idle until the next interval.
    private final Object pollLock = new Object();
    private boolean polling;
    private boolean wakeRequested;
    private ScheduledFuture<?> nextPoll;
    private long lastPollStartNanos;
    private volatile boolean lastPollFoundWork;

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
        
        synchronized (pollLock) {
            polling = false;
            wakeRequested = false;
            nextPoll = null;
        }
        pollerScheduler.execute(this::pollAndScheduleNext);

        startHeartbeatSchedulerIfNeeded();

        log.info("Task poller started");
    }

    /**
     * Stop the poller gracefully
     */
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

        // Keep the executor alive so the poller can be started again. Continue
        // heartbeats while active tasks drain, then stop the heartbeat scheduler.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(shutdownTimeoutSeconds());
        while (hasInFlightWork() && System.nanoTime() < deadline) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (hasInFlightWork()) {
            log.warn("Some tasks are still running after the graceful stop timeout; heartbeats remain active");
        } else {
            stopHeartbeatScheduler();
        }

        log.info("Task poller stopped");
    }

    /** Shut down resources when the Spring application context closes. */
    @PreDestroy
    public synchronized void shutdown() {
        stop();
        taskProcessorExecutor.shutdown();
        try {
            if (!taskProcessorExecutor.awaitTermination(shutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                log.warn("Some tasks did not complete during application shutdown; forcing shutdown");
                // Release first: interrupting makes well-behaved handlers throw, and the
                // resulting failTask would otherwise burn an attempt (or mark the task FAILED).
                // Once released, those late failure reports are ignored by the attempt guard.
                releaseUnfinishedTasks();
                taskProcessorExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            releaseUnfinishedTasks();
            taskProcessorExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            stopHeartbeatScheduler();
        }
    }

    /**
     * Hand tasks that are still running when the executor is about to be forced down straight back to
     * the queue, rather than leaving them PROCESSING until stale-task recovery notices.
     */
    private void releaseUnfinishedTasks() {
        if (processingTasks.isEmpty()) {
            return;
        }
        List<UUID> ids = new ArrayList<>();
        processingTasks.values().forEach(handle -> ids.add(handle.task().getId()));
        try {
            int released = taskQueueService.releaseTasks(ids);
            taskMetrics.recordTasksReleased(released);
            log.info("Released {} unfinished tasks back to the queue at shutdown", released);
        } catch (Exception e) {
            log.warn("Could not release unfinished tasks at shutdown; stale-task recovery will reclaim them: {}",
                    e.getMessage());
        }
        processingTasks.clear();
    }

    private int shutdownTimeoutSeconds() {
        return Math.max(0, properties.getPoller().getShutdownTimeoutSeconds());
    }

    private void startHeartbeatSchedulerIfNeeded() {
        if (heartbeatScheduler != null && !heartbeatScheduler.isShutdown()) {
            return;
        }
        // Two threads so the (database-bound) heartbeat write can never delay the timeout
        // watchdog, and vice versa.
        AtomicInteger threadCounter = new AtomicInteger();
        heartbeatScheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "task-heartbeat-" + threadCounter.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        heartbeatScheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                properties.getHeartbeat().getIntervalMs(),
                properties.getHeartbeat().getIntervalMs(),
                TimeUnit.MILLISECONDS
        );
        long timeoutCheckMs = Math.max(1, properties.getHeartbeat().getTimeoutCheckIntervalMs());
        heartbeatScheduler.scheduleWithFixedDelay(
                this::enforceTimeouts, timeoutCheckMs, timeoutCheckMs, TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeatScheduler() {
        ScheduledExecutorService scheduler = heartbeatScheduler;
        heartbeatScheduler = null;
        if (scheduler == null) {
            return;
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private boolean hasInFlightWork() {
        return !processingTasks.isEmpty()
                || taskProcessorExecutor.getActiveCount() > 0
                || !taskProcessorExecutor.getQueue().isEmpty();
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
            // The executor uses a bounded work queue, so submissions beyond the
            // core pool size queue instead of causing it to grow toward maxPoolSize.
            // Only claim tasks that can start without adding avoidable executor delay.
            int availableCapacity = taskProcessorExecutor.getCorePoolSize()
                    - taskProcessorExecutor.getActiveCount()
                    - taskProcessorExecutor.getQueue().size();
            
            if (availableCapacity <= 0) {
                log.debug("Thread pool at capacity, skipping poll");
                return;
            }

            int fetchSize = Math.min(currentBatchSize, availableCapacity);
            List<Task> tasks = taskQueueService.dequeueTasks(fetchSize, availableCapacityByType());

            lastPollFoundWork = !tasks.isEmpty();
            if (tasks.isEmpty()) {
                // Adaptive polling: increase interval when queue is empty
                if (properties.getPoller().isAdaptivePolling() && processingTasks.isEmpty()) {
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
                AtomicReference<ProcessingHandle> handleReference = new AtomicReference<>();

                FutureTask<Void> future = new FutureTask<>(() -> {
                    try {
                        taskProcessorService.processTask(task);
                    } catch (Exception ex) {
                        log.error("Unexpected error processing task {}: {}", task.getId(), ex.getMessage());
                    } finally {
                        ProcessingHandle handle = handleReference.get();
                        if (handle != null) {
                            processingTasks.remove(taskKey, handle);
                        }
                        requestEarlyPoll();
                    }
                    return null;
                });
                // Publish tracking before execution: a short task may finish
                // immediately, in which case its finally block must be able to
                // remove an already-present entry.
                ProcessingHandle handle = new ProcessingHandle(task, future);
                handleReference.set(handle);
                processingTasks.put(taskKey, handle);
                try {
                    taskProcessorExecutor.execute(future);
                } catch (RejectedExecutionException rejected) {
                    // Claimed but could not be queued (executor shutting down or saturated):
                    // give it straight back instead of leaving it PROCESSING with no worker.
                    processingTasks.remove(taskKey, handle);
                    log.warn("Executor rejected task {}, releasing it back to the queue", task.getId());
                    taskMetrics.recordTasksReleased(taskQueueService.releaseTasks(List.of(task.getId())));
                }
            }

            taskMetrics.recordTasksPolled(tasks.size());
            log.debug("Submitted {} tasks for processing", tasks.size());

        } catch (Exception e) {
            log.error("Error during poll cycle: {}", e.getMessage(), e);
        }
    }

    private Map<String, Integer> availableCapacityByType() {
        if (properties.getPoller().getMaxConcurrentByType().isEmpty()) {
            return Map.of();
        }
        Map<String, Integer> activeByType = new ConcurrentHashMap<>();
        processingTasks.values().forEach(handle ->
                activeByType.merge(handle.task().getTaskType(), 1, Integer::sum));
        Map<String, Integer> available = new ConcurrentHashMap<>();
        properties.getPoller().getMaxConcurrentByType().forEach((taskType, maxActive) ->
                available.put(taskType, Math.max(0, maxActive - activeByType.getOrDefault(taskType, 0))));
        return available;
    }

    /** Run one poll cycle, then schedule the next one using the latest interval. */
    private void pollAndScheduleNext() {
        synchronized (pollLock) {
            polling = true;
            wakeRequested = false;
            nextPoll = null;
            lastPollStartNanos = System.nanoTime();
        }
        try {
            pollAndProcess();
        } finally {
            synchronized (pollLock) {
                polling = false;
                scheduleNextPollLocked(wakeRequested ? earlyPollDelayMs() : currentPollInterval.get());
            }
        }
    }

    /**
     * Called when a task finishes and frees a worker slot. If the queue had work on the last
     * poll, poll again right away (bounded by the minimum poll interval) so the slot does not
     * sit idle until the next scheduled poll. Skipped while the queue is empty so adaptive
     * backoff still protects the database.
     */
    private void requestEarlyPoll() {
        if (!lastPollFoundWork) {
            return;
        }
        synchronized (pollLock) {
            if (!running.get() || pollerScheduler == null) {
                return;
            }
            if (polling) {
                wakeRequested = true;
                return;
            }
            long delay = earlyPollDelayMs();
            if (nextPoll == null || nextPoll.getDelay(TimeUnit.MILLISECONDS) > delay) {
                scheduleNextPollLocked(delay);
            }
        }
    }

    private long earlyPollDelayMs() {
        long sinceLastPollMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastPollStartNanos);
        return Math.max(0, properties.getPoller().getMinPollIntervalMs() - sinceLastPollMs);
    }

    private void scheduleNextPollLocked(long delayMs) {
        ScheduledExecutorService scheduler = pollerScheduler;
        if (!running.get() || scheduler == null || scheduler.isShutdown()) {
            return;
        }
        if (nextPoll != null) {
            nextPoll.cancel(false);
        }
        try {
            nextPoll = scheduler.schedule(this::pollAndScheduleNext, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            nextPoll = null;
        }
    }

    /** Send one batched heartbeat for every task this worker is processing. */
    private void sendHeartbeats() {
        try {
            if (processingTasks.isEmpty()) {
                return;
            }
            List<UUID> ids = new ArrayList<>(processingTasks.size());
            processingTasks.values().forEach(handle -> ids.add(handle.task().getId()));
            taskQueueService.updateHeartbeats(ids);
            log.debug("Sent heartbeats for {} tasks", ids.size());
        } catch (Exception e) {
            log.error("Error sending heartbeats: {}", e.getMessage(), e);
        }
    }

    /** Cancel and fail tasks that have overrun their execution timeout. */
    private void enforceTimeouts() {
        try {
            for (Map.Entry<String, ProcessingHandle> entry : processingTasks.entrySet()) {
                Task task = entry.getValue().task();
                if (!hasTimedOut(task)) {
                    continue;
                }
                // Atomically claim this task so a concurrent normal completion
                // doesn't also try to handle it (see TaskQueueService's
                // PROCESSING-status guard for the corresponding DB-side safeguard).
                ProcessingHandle claimed = processingTasks.remove(entry.getKey(), entry.getValue())
                        ? entry.getValue() : null;
                if (claimed != null) {
                    int timeoutSeconds = effectiveTimeoutSeconds(task);
                    log.warn("Task {} exceeded timeout of {}s, cancelling", task.getId(), timeoutSeconds);
                    claimed.future().cancel(true);
                    taskMetrics.recordTaskTimedOut(task.getTaskType());
                    taskMetrics.recordAttempt(task.getTaskType(), "timeout",
                            System.currentTimeMillis() - task.getStartedAt().toEpochMilli());
                    try {
                        taskQueueService.failTask(task, "Task exceeded timeout of " + timeoutSeconds + "s");
                    } catch (Exception e) {
                        log.error("Could not record timeout for task {}: {}", task.getId(), e.getMessage(), e);
                    }
                    requestEarlyPoll();
                }
            }
        } catch (Exception e) {
            log.error("Error enforcing task timeouts: {}", e.getMessage(), e);
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
