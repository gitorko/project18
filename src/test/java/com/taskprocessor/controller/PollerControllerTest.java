package com.taskprocessor.controller;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.PollerConfigRequest;
import com.taskprocessor.service.TaskPollerService;
import org.junit.jupiter.api.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PollerControllerTest {
    @Test void reportsAndUpdatesConfigurationAndLifecycle() {
        TaskPollerService poller = mock(TaskPollerService.class);
        TaskProcessorProperties properties = new TaskProcessorProperties();
        properties.setWorkerId("worker");
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 4, 1, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        try {
            PollerController controller = new PollerController(poller, properties, executor);
            when(poller.getCurrentBatchSize()).thenReturn(8);
            when(poller.getCurrentPollInterval()).thenReturn(500L);
            when(poller.isEnabled()).thenReturn(true);
            var config = controller.getConfig().getBody();
            assertEquals(2, config.getPoolSize());
            assertEquals(8, config.getBatchSize());

            controller.updateConfig(PollerConfigRequest.builder().poolSize(3).batchSize(12)
                    .pollIntervalMs(750L).enabled(false).build());
            verify(poller).setPoolSize(3);
            verify(poller).setBatchSize(12);
            verify(poller).setPollInterval(750L);
            verify(poller).setEnabled(false);

            when(poller.isRunning()).thenReturn(false, true, true, false);
            assertEquals("Poller started", controller.startPoller().getBody());
            verify(poller).start();
            assertEquals("Poller is already running", controller.startPoller().getBody());
            assertEquals("Poller stopped", controller.stopPoller().getBody());
            verify(poller).stop();
            when(poller.isRunning()).thenReturn(false);
            assertEquals("Poller is already stopped", controller.stopPoller().getBody());
        } finally {
            executor.shutdownNow();
        }
    }
}
