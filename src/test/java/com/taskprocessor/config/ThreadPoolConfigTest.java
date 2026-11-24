package com.taskprocessor.config;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ThreadPoolConfigTest {
    @Test void createsConfiguredInstrumentedPoolWithNamedDaemonThreads() {
        TaskProcessorProperties properties = new TaskProcessorProperties();
        properties.getPoller().setPoolSize(2);
        properties.getPoller().setMaxPoolSize(4);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        var executor = new ThreadPoolConfig(properties, registry).taskProcessorExecutor();
        try {
            assertEquals(2, executor.getCorePoolSize());
            assertEquals(4, executor.getMaximumPoolSize());
            var thread = executor.getThreadFactory().newThread(() -> { });
            assertTrue(thread.isDaemon());
            assertTrue(thread.getName().startsWith("task-processor-"));
            assertFalse(registry.getMeters().isEmpty());
        } finally {
            executor.shutdownNow();
            registry.close();
        }
    }
}
