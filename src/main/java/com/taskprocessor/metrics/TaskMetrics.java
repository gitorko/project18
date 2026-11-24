package com.taskprocessor.metrics;

import io.micrometer.core.instrument.*;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Metrics for monitoring task processing.
 * Exposes metrics via Spring Actuator / Micrometer.
 */
@Component
@RequiredArgsConstructor
public class TaskMetrics {

    private final MeterRegistry meterRegistry;
    private final ThreadPoolExecutor taskProcessorExecutor;

    // Counters
    private Counter tasksEnqueuedCounter;
    private Counter tasksCompletedCounter;
    private Counter tasksFailedCounter;
    private Counter tasksPolledCounter;

    // Gauges
    private final AtomicInteger activeProcessingCount = new AtomicInteger(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
    private final AtomicLong completedTaskCount = new AtomicLong(0);
    private final Map<String, AtomicLong> completedByType = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> processingTimeByTypeMs = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<Long> completionTimesNanos = new ConcurrentLinkedDeque<>();

    // Timer for processing duration
    private Timer processingTimer;

    @PostConstruct
    public void init() {
        // Task counters
        tasksEnqueuedCounter = Counter.builder("tasks.enqueued")
                .description("Total number of tasks enqueued")
                .register(meterRegistry);

        tasksCompletedCounter = Counter.builder("tasks.completed")
                .description("Total number of tasks completed successfully")
                .register(meterRegistry);

        tasksFailedCounter = Counter.builder("tasks.failed")
                .description("Total number of tasks that failed")
                .register(meterRegistry);

        tasksPolledCounter = Counter.builder("tasks.polled")
                .description("Total number of tasks polled from queue")
                .register(meterRegistry);

        // Processing timer
        processingTimer = Timer.builder("tasks.processing.duration")
                .description("Time taken to process tasks")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);

        // Gauges
        Gauge.builder("tasks.processing.active", activeProcessingCount, AtomicInteger::get)
                .description("Number of tasks currently being processed")
                .register(meterRegistry);

        Gauge.builder("tasks.processing.pool.size", taskProcessorExecutor, ThreadPoolExecutor::getPoolSize)
                .description("Current thread pool size")
                .register(meterRegistry);

        Gauge.builder("tasks.processing.pool.active", taskProcessorExecutor, ThreadPoolExecutor::getActiveCount)
                .description("Number of active threads in pool")
                .register(meterRegistry);

        Gauge.builder("tasks.processing.queue.size", taskProcessorExecutor, 
                e -> e.getQueue().size())
                .description("Number of tasks waiting in executor queue")
                .register(meterRegistry);

        // Processing rate (tasks per minute)
        Gauge.builder("tasks.processing.rate", this, TaskMetrics::getProcessingRatePerMinute)
                .description("Task processing rate per minute")
                .register(meterRegistry);

        // Average processing time
        Gauge.builder("tasks.processing.avg.time.ms", this, TaskMetrics::getAverageProcessingTimeMs)
                .description("Average task processing time in milliseconds")
                .register(meterRegistry);
    }

    /**
     * Record a task being enqueued
     */
    public void recordTaskEnqueued(String taskType) {
        tasksEnqueuedCounter.increment();
        Counter.builder("tasks.enqueued.by.type")
                .tag("type", taskType)
                .register(meterRegistry)
                .increment();
    }

    /**
     * Record a task being completed
     */
    public void recordTaskCompleted(String taskType, long durationMs) {
        tasksCompletedCounter.increment();
        totalProcessingTimeMs.addAndGet(durationMs);
        completedTaskCount.incrementAndGet();
        completedByType.computeIfAbsent(taskType, ignored -> new AtomicLong()).incrementAndGet();
        processingTimeByTypeMs.computeIfAbsent(taskType, ignored -> new AtomicLong()).addAndGet(durationMs);
        long completionTime = System.nanoTime();
        completionTimesNanos.addLast(completionTime);
        pruneCompletionTimes(completionTime);
        
        processingTimer.record(java.time.Duration.ofMillis(durationMs));
        
        Counter.builder("tasks.completed.by.type")
                .tag("type", taskType)
                .register(meterRegistry)
                .increment();

        Timer.builder("tasks.processing.duration.by.type")
                .tag("type", taskType)
                .register(meterRegistry)
                .record(java.time.Duration.ofMillis(durationMs));
    }

    /**
     * Record a task failing
     */
    public void recordTaskFailed(String taskType) {
        tasksFailedCounter.increment();
        Counter.builder("tasks.failed.by.type")
                .tag("type", taskType)
                .register(meterRegistry)
                .increment();
    }

    /**
     * Record tasks being polled
     */
    public void recordTasksPolled(int count) {
        tasksPolledCounter.increment(count);
    }

    /**
     * Increment active processing count
     */
    public void incrementActiveProcessing() {
        activeProcessingCount.incrementAndGet();
    }

    /**
     * Decrement active processing count
     */
    public void decrementActiveProcessing() {
        activeProcessingCount.decrementAndGet();
    }

    /**
     * Get the processing rate per minute
     */
    public double getProcessingRatePerMinute() {
        long now = System.nanoTime();
        pruneCompletionTimes(now);
        Long first = completionTimesNanos.peekFirst();
        if (first == null) {
            return 0;
        }
        long elapsed = Math.min(TimeUnit.MINUTES.toNanos(1), now - first);
        // Avoid a noisy instantaneous rate when the process has only just started.
        elapsed = Math.max(TimeUnit.SECONDS.toNanos(1), elapsed);
        return completionTimesNanos.size() * (double) TimeUnit.MINUTES.toNanos(1) / elapsed;
    }

    private void pruneCompletionTimes(long now) {
        long windowStart = now - TimeUnit.MINUTES.toNanos(1);
        Long oldest;
        while ((oldest = completionTimesNanos.peekFirst()) != null && oldest < windowStart) {
            completionTimesNanos.removeFirstOccurrence(oldest);
        }
    }

    /**
     * Get the average processing time in milliseconds
     */
    public double getAverageProcessingTimeMs() {
        long count = completedTaskCount.get();
        if (count == 0) {
            return 0;
        }
        return (double) totalProcessingTimeMs.get() / count;
    }

    public double getAverageProcessingTimeMs(String taskType) {
        AtomicLong count = completedByType.get(taskType);
        AtomicLong time = processingTimeByTypeMs.get(taskType);
        return count == null || time == null || count.get() == 0 ? 0 : (double) time.get() / count.get();
    }

    /**
     * Get the number of active workers (threads actively processing)
     */
    public int getActiveWorkerCount() {
        return taskProcessorExecutor.getActiveCount();
    }

    /**
     * Get the current pool size
     */
    public int getCurrentPoolSize() {
        return taskProcessorExecutor.getPoolSize();
    }

    public int getConfiguredPoolSize() {
        return taskProcessorExecutor.getCorePoolSize();
    }
}
