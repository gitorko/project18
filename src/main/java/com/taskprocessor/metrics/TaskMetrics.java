package com.taskprocessor.metrics;

import io.micrometer.core.instrument.*;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
        // Calculate based on the timer's mean rate
        return processingTimer.count() > 0 ? 
                processingTimer.count() / (processingTimer.totalTime(java.util.concurrent.TimeUnit.MINUTES) + 0.001) : 0;
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
}
