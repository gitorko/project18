package com.taskprocessor.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TaskMetricsTest {
    @Test void recordsCompletionFailureAndExecutorMeasurements() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 1, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        try {
            TaskMetrics metrics = new TaskMetrics(registry, executor);
            metrics.init();
            metrics.recordTaskEnqueued("email");
            metrics.recordTasksPolled(3);
            metrics.recordTaskCompleted("email", 120);
            metrics.recordTaskCompleted("email", 80);
            metrics.recordTaskFailed("email");
            metrics.incrementActiveProcessing();
            assertEquals(100.0, metrics.getAverageProcessingTimeMs(), 0.001);
            assertEquals(100.0, metrics.getAverageProcessingTimeMs("email"), 0.001);
            assertEquals(0.0, metrics.getAverageProcessingTimeMs("unknown"), 0.001);
            assertEquals(1, registry.get("tasks.enqueued").counter().count());
            assertEquals(3, registry.get("tasks.polled").counter().count());
            assertEquals(2, registry.get("tasks.completed").counter().count());
            assertEquals(1, registry.get("tasks.failed").counter().count());
            assertEquals(1.0, registry.get("tasks.processing.active").gauge().value(), 0.001);
            metrics.decrementActiveProcessing();
            assertTrue(registry.get("tasks.processing.duration").timer().totalTime(TimeUnit.MILLISECONDS) >= 200);
            assertTrue(metrics.getProcessingRatePerMinute() > 0);
            assertEquals(2, metrics.getConfiguredPoolSize());
        } finally {
            executor.shutdownNow();
            registry.close();
        }
    }

    @Test void recordsQueueWaitAttemptOutcomesAndLifecycleCounters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        try {
            TaskMetrics metrics = new TaskMetrics(registry, executor);
            metrics.init();
            metrics.recordQueueWait("email", 250);
            metrics.recordAttempt("email", "completed", 40);
            metrics.recordAttempt("email", "retried", 10);
            metrics.recordTaskRetried("email");
            metrics.recordTaskTimedOut("report");
            metrics.recordStaleRecovery(2, 1);
            metrics.recordTasksReleased(3);
            metrics.recordTasksReleased(0);
            metrics.recordTasksPurged(5);

            assertEquals(250, registry.get("tasks.queue.wait.duration").tag("type", "email").timer()
                    .totalTime(TimeUnit.MILLISECONDS), 0.001);
            assertEquals(1, registry.get("tasks.attempt.duration").tag("outcome", "completed").timer().count());
            assertEquals(1, registry.get("tasks.attempt.duration").tag("outcome", "retried").timer().count());
            assertEquals(1, registry.get("tasks.retried").tag("type", "email").counter().count());
            assertEquals(1, registry.get("tasks.timeouts").tag("type", "report").counter().count());
            assertEquals(2, registry.get("tasks.stale.reset").counter().count());
            assertEquals(1, registry.get("tasks.stale.failed").counter().count());
            assertEquals(1, registry.get("tasks.failed").counter().count(), "stale failures count as failed tasks");
            assertEquals(3, registry.get("tasks.released").counter().count());
            assertEquals(5, registry.get("tasks.purged").counter().count());
        } finally {
            executor.shutdownNow();
            registry.close();
        }
    }
}
