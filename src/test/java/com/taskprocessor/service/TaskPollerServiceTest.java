package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.metrics.TaskMetrics;
import org.junit.jupiter.api.Test;

import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final TaskQueueService queue = mock(TaskQueueService.class);
    private final TaskProcessorService processor = mock(TaskProcessorService.class);
    private final TaskProcessorProperties properties = new TaskProcessorProperties();
    private ThreadPoolExecutor executor;
    private TaskPollerService poller;

    private void startPoller(int poolSize) {
        executor = new ThreadPoolExecutor(poolSize, poolSize, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10));
        poller = new TaskPollerService(queue, processor, properties, executor, mock(TaskMetrics.class));
        poller.init();
    }

    @org.junit.jupiter.api.AfterEach void tearDown() {
        if (poller != null) {
            poller.stop();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private Task runningTask(int timeoutSeconds, Instant startedAt) {
        return Task.builder().id(UUID.randomUUID()).taskType("t").status(TaskStatus.PROCESSING)
                .attemptCount(1).maxAttempts(1).timeoutSeconds(timeoutSeconds).startedAt(startedAt).build();
    }

    @Test void heartbeatsAreSentAsOneBatchedUpdate() throws Exception {
        properties.getHeartbeat().setIntervalMs(30);
        properties.getPoller().setBatchSize(2);
        Task t1 = runningTask(60, Instant.now()), t2 = runningTask(60, Instant.now());
        when(queue.dequeueTasks(anyInt(), anyMap())).thenReturn(List.of(t1, t2)).thenReturn(List.of());
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(inv -> { release.await(); return null; }).when(processor).processTask(any());

        startPoller(2);
        try {
            verify(queue, timeout(2000).atLeastOnce()).updateHeartbeats(argThat(ids ->
                    ids.size() == 2 && ids.contains(t1.getId()) && ids.contains(t2.getId())));
            verify(queue, never()).updateHeartbeat(any());
        } finally {
            release.countDown();
        }
    }

    @Test void timeoutIsEnforcedEvenWhenHeartbeatsAreInfrequent() throws Exception {
        properties.getHeartbeat().setIntervalMs(600_000);
        properties.getHeartbeat().setTimeoutCheckIntervalMs(20);
        Task overdue = runningTask(1, Instant.now().minusSeconds(30));
        when(queue.dequeueTasks(anyInt(), anyMap())).thenReturn(List.of(overdue)).thenReturn(List.of());
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch started = new CountDownLatch(1);
        doAnswer(inv -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
            return null;
        }).when(processor).processTask(any());

        startPoller(1);

        verify(queue, timeout(2000)).failTask(eq(overdue), contains("exceeded timeout"));
        assertTrue(started.await(2, TimeUnit.SECONDS));
        long deadline = System.currentTimeMillis() + 2000;
        while (!interrupted.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(interrupted.get(), "the overrunning task must be interrupted");
    }

    @Test void finishingATaskTriggersAnEarlyPollWhileTheQueueHasWork() {
        properties.getPoller().setAdaptivePolling(false);
        properties.getPoller().setPollIntervalMs(600_000);
        properties.getPoller().setMinPollIntervalMs(20);
        properties.getHeartbeat().setIntervalMs(600_000);
        properties.getHeartbeat().setTimeoutCheckIntervalMs(600_000);
        when(queue.dequeueTasks(anyInt(), anyMap()))
                .thenReturn(List.of(runningTask(60, Instant.now())))
                .thenReturn(List.of(runningTask(60, Instant.now())))
                .thenReturn(List.of());

        startPoller(1);

        // With a 10 minute poll interval, only completion-driven polls can reach three calls.
        verify(queue, timeout(3000).times(3)).dequeueTasks(anyInt(), anyMap());
        verify(processor, timeout(3000).times(2)).processTask(any());
    }

    @Test void emptyQueueDoesNotTriggerEarlyPolls() throws Exception {
        properties.getPoller().setAdaptivePolling(false);
        properties.getPoller().setPollIntervalMs(600_000);
        properties.getPoller().setMinPollIntervalMs(10);
        when(queue.dequeueTasks(anyInt(), anyMap())).thenReturn(List.of());

        startPoller(1);

        verify(queue, timeout(2000).times(1)).dequeueTasks(anyInt(), anyMap());
        Thread.sleep(200);
        verify(queue, times(1)).dequeueTasks(anyInt(), anyMap());
    }

    @Test void tasksRejectedByTheExecutorAreReleasedBackToTheQueue() {
        Task claimed = runningTask(60, Instant.now());
        when(queue.dequeueTasks(anyInt(), anyMap())).thenReturn(List.of(claimed)).thenReturn(List.of());
        executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10));
        executor.shutdown();
        poller = new TaskPollerService(queue, processor, properties, executor, mock(TaskMetrics.class));
        poller.init();

        verify(queue, timeout(2000)).releaseTasks(List.of(claimed.getId()));
        verify(processor, never()).processTask(any());
        assertEquals(0, poller.getProcessingTaskCount());
    }

    @Test void shutdownReleasesTasksThatIgnoreInterruption() throws Exception {
        properties.getPoller().setShutdownTimeoutSeconds(1);
        Task stuck = runningTask(600, Instant.now());
        when(queue.dequeueTasks(anyInt(), anyMap())).thenReturn(List.of(stuck)).thenReturn(List.of());
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        doAnswer(inv -> {
            started.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    // keep running: simulates a handler that cannot be interrupted
                }
            }
            return null;
        }).when(processor).processTask(any());

        startPoller(1);
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            poller.shutdown();
            verify(queue).releaseTasks(List.of(stuck.getId()));
            assertEquals(0, poller.getProcessingTaskCount());
        } finally {
            release.countDown();
        }
    }

    @Test void interruptedHandlersDoNotFailTasksThatShutdownReleased() throws Exception {
        properties.getPoller().setShutdownTimeoutSeconds(1);
        Task running = runningTask(600, Instant.now());
        when(queue.dequeueTasks(anyInt(), anyMap())).thenReturn(List.of(running)).thenReturn(List.of());
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        // Behaves like TaskProcessorService: an interrupted handler reports the task as failed.
        doAnswer(inv -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                queue.failTask(running, "interrupted");
            }
            return null;
        }).when(processor).processTask(any());
        // Release must happen before the interrupt is delivered.
        when(queue.releaseTasks(any())).thenAnswer(inv -> {
            assertEquals(1, interrupted.getCount(), "tasks must be released before handlers are interrupted");
            return 1;
        });

        startPoller(1);
        assertTrue(started.await(2, TimeUnit.SECONDS));
        poller.shutdown();

        assertTrue(interrupted.await(2, TimeUnit.SECONDS));
        var order = inOrder(queue);
        order.verify(queue).releaseTasks(List.of(running.getId()));
        order.verify(queue).failTask(eq(running), any());
    }
}
