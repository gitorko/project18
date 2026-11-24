package com.taskprocessor.service;

import com.taskprocessor.entity.Task;
import com.taskprocessor.metrics.TaskMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskProcessorServiceTest {
    @Mock TaskQueueService queue;
    @Mock TaskMetrics metrics;
    private TaskHandler selectedHandler;
    private TaskProcessorService service;

    @BeforeEach void setUp() {
        selectedHandler = mock(TaskHandler.class);
        when(selectedHandler.getTaskType()).thenReturn("email");
        service = new TaskProcessorService(queue, List.of(selectedHandler), metrics);
        service.init();
    }

    @Test void routesAndCompletesRegisteredType() throws Exception {
        Task task = Task.builder().taskType("email").build();
        service.processTask(task);
        verify(selectedHandler).handle(task);
        verify(queue).completeTask(task.getId(), task.getAttemptCount());
        verify(queue, never()).failTask(any(), anyInt(), anyString());
        verify(metrics).incrementActiveProcessing();
        verify(metrics).decrementActiveProcessing();
    }

    @Test void routesUnknownTypeToDefaultAndFailsOnHandlerException() throws Exception {
        TaskHandler defaultHandler = mock(TaskHandler.class);
        when(defaultHandler.getTaskType()).thenReturn(DefaultTaskHandler.DEFAULT_TASK_TYPE);
        service = new TaskProcessorService(queue, List.of(selectedHandler, defaultHandler), metrics);
        service.init();
        Task task = Task.builder().taskType("unknown").build();
        doThrow(new IllegalStateException("boom")).when(defaultHandler).handle(task);
        service.processTask(task);
        verify(defaultHandler).handle(task);
        verify(queue).failTask(task.getId(), task.getAttemptCount(), "boom");
        verify(queue, never()).completeTask(any(), anyInt());
        verify(metrics).decrementActiveProcessing();
    }

    @Test void reportsRegisteredAndFallbackHandlers() {
        assertTrue(service.hasHandler("email"));
        assertFalse(service.hasHandler("unknown"));
        assertSame(selectedHandler, service.getHandler("email"));
        assertNull(service.getHandler("unknown"));
    }

    @Test void failsTaskWhenNoHandlerIsRegistered() {
        service = new TaskProcessorService(queue, List.of(), metrics);
        service.init();
        Task task = Task.builder().taskType("unhandled").build();
        service.processTask(task);
        verify(queue).failTask(task.getId(), task.getAttemptCount(), "No handler found for task type: unhandled");
        verify(metrics).decrementActiveProcessing();
    }
}
