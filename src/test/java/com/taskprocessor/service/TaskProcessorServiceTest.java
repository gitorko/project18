package com.taskprocessor.service;

import com.taskprocessor.entity.Task;
import com.taskprocessor.metrics.TaskMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
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
        Task task = Task.builder().id(java.util.UUID.randomUUID()).taskType("email").attemptCount(1).build();
        service.processTask(task);
        verify(selectedHandler).handle(task);
        verify(queue).completeTask(task);
        verify(queue, never()).failTask(any(), anyString());
        verify(metrics).recordAttempt(eq("email"), eq("completed"), anyLong());
        verify(metrics).incrementActiveProcessing();
        verify(metrics).decrementActiveProcessing();
    }

    @Test void routesUnknownTypeToDefaultAndFailsOnHandlerException() throws Exception {
        TaskHandler defaultHandler = mock(TaskHandler.class);
        when(defaultHandler.getTaskType()).thenReturn(DefaultTaskHandler.DEFAULT_TASK_TYPE);
        service = new TaskProcessorService(queue, List.of(selectedHandler, defaultHandler), metrics);
        service.init();
        Task task = Task.builder().id(java.util.UUID.randomUUID()).taskType("unknown").attemptCount(1).maxAttempts(1).build();
        doThrow(new IllegalStateException("boom")).when(defaultHandler).handle(task);
        service.processTask(task);
        verify(defaultHandler).handle(task);
        verify(queue).failTask(task, "boom");
        verify(queue, never()).completeTask(any());
        verify(metrics).recordAttempt(eq("unknown"), eq("failed"), anyLong());
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
        Task task = Task.builder().id(java.util.UUID.randomUUID()).taskType("unhandled").attemptCount(1).maxAttempts(1).build();
        service.processTask(task);
        verify(queue).failTask(task, "No handler found for task type: unhandled");
        verify(metrics).decrementActiveProcessing();
    }

    @Test void handlerRunsWithTaskAndCorrelationIdInTheLoggingContextAndCleansUp() throws Exception {
        Task task = Task.builder().id(java.util.UUID.randomUUID()).taskType("email").attemptCount(2)
                .correlationId("req-7").build();
        java.util.Map<String, String> seen = new java.util.HashMap<>();
        doAnswer(inv -> {
            seen.put("taskId", org.slf4j.MDC.get("taskId"));
            seen.put("attempt", org.slf4j.MDC.get("attempt"));
            seen.put("correlationId", org.slf4j.MDC.get("correlationId"));
            return null;
        }).when(selectedHandler).handle(task);

        service.processTask(task);

        assertEquals(task.getId().toString(), seen.get("taskId"));
        assertEquals("2", seen.get("attempt"));
        assertEquals("req-7", seen.get("correlationId"));
        assertNull(org.slf4j.MDC.get("taskId"));
        assertNull(org.slf4j.MDC.get("correlationId"));
    }

    @Test void recordsRetriedOutcomeWhenAttemptsRemain() throws Exception {
        Task task = Task.builder().id(java.util.UUID.randomUUID()).taskType("email").attemptCount(1).maxAttempts(3).build();
        doThrow(new IllegalStateException("flaky")).when(selectedHandler).handle(task);
        service.processTask(task);
        verify(metrics).recordAttempt(eq("email"), eq("retried"), anyLong());
    }
}
