package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.metrics.TaskMetrics;
import org.junit.jupiter.api.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TaskPollerServiceTest {
    @Test void initializesDisabledAndAppliesRuntimeSettings() {
        TaskProcessorProperties properties = new TaskProcessorProperties();
        properties.getPoller().setEnabled(false);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 4, 1, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        try {
            TaskPollerService poller = new TaskPollerService(mock(TaskQueueService.class),
                    mock(TaskProcessorService.class), properties, executor, mock(TaskMetrics.class));
            poller.init();
            assertFalse(poller.isRunning());
            assertFalse(poller.isEnabled());
            assertEquals(10, poller.getCurrentBatchSize());
            poller.setEnabled(true);
            poller.setBatchSize(7);
            poller.setPollInterval(250);
            poller.setPoolSize(3);
            assertTrue(poller.isEnabled());
            assertEquals(7, poller.getCurrentBatchSize());
            assertEquals(250, poller.getCurrentPollInterval());
            assertEquals(3, executor.getCorePoolSize());
            assertFalse(poller.isRunning());

            poller.start();
            assertTrue(poller.isRunning());
            poller.stop();
            assertFalse(poller.isRunning());
            assertFalse(executor.isShutdown(), "stopping the poller must allow it to be restarted");

            poller.start();
            assertTrue(poller.isRunning());
            poller.stop();
        } finally {
            executor.shutdownNow();
        }
    }
}
