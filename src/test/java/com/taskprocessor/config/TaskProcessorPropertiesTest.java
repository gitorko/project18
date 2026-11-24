package com.taskprocessor.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskProcessorPropertiesTest {
    @Test void providesWorkerAndNestedConfigurationDefaults() {
        TaskProcessorProperties properties = new TaskProcessorProperties();
        assertNotNull(properties.getWorkerId());
        assertTrue(properties.getPoller().isEnabled());
        assertEquals(5, properties.getPoller().getPoolSize());
        assertEquals(20, properties.getPoller().getMaxPoolSize());
        assertEquals(10, properties.getPoller().getBatchSize());
        assertEquals(1000, properties.getPoller().getPollIntervalMs());
        assertEquals(5, properties.getPoller().getFairnessCandidateMultiplier());
        assertEquals(200, properties.getPoller().getMaxFairnessCandidates());
        assertEquals(120, properties.getPoller().getDefaultTaskTimeoutSeconds());
        assertEquals(5000, properties.getHeartbeat().getIntervalMs());
        assertEquals(30000, properties.getHeartbeat().getStaleThresholdMs());
        assertEquals(10000, properties.getHeartbeat().getRecoveryIntervalMs());
    }
}
