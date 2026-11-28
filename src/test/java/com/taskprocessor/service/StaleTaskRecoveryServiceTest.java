package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.metrics.TaskMetrics;
import com.taskprocessor.repository.TaskRepository;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class StaleTaskRecoveryServiceTest {
    @Test void recoversRetryableAndExhaustedRows() {
        TaskRepository repository = mock(TaskRepository.class);
        TaskProcessorProperties properties = new TaskProcessorProperties();
        TaskMetrics metrics = mock(TaskMetrics.class);
        when(repository.resetStaleTasks(any(), any())).thenReturn(2);
        when(repository.markExhaustedTasksAsFailed(any(), any())).thenReturn(1);
        new StaleTaskRecoveryService(repository, properties, metrics).recoverStaleTasks();
        verify(repository).resetStaleTasks(any(), any());
        verify(repository).markExhaustedTasksAsFailed(any(), any());
        verify(metrics).recordStaleRecovery(2, 1);
    }

    @Test void handlesRepositoryFailureWithoutPropagating() {
        TaskRepository repository = mock(TaskRepository.class);
        TaskMetrics metrics = mock(TaskMetrics.class);
        when(repository.resetStaleTasks(any(), any())).thenThrow(new IllegalStateException("database down"));
        new StaleTaskRecoveryService(repository, new TaskProcessorProperties(), metrics).recoverStaleTasks();
        verify(repository, never()).markExhaustedTasksAsFailed(any(), any());
    }
}
