package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TaskRetentionServiceTest {
    private final TaskRepository repository = mock(TaskRepository.class);
    private final TaskProcessorProperties properties = new TaskProcessorProperties();
    private final TransactionTemplate tx = mock(TransactionTemplate.class);
    private final com.taskprocessor.metrics.TaskMetrics metrics = mock(com.taskprocessor.metrics.TaskMetrics.class);
    private TaskRetentionService service;

    @BeforeEach void setUp() {
        when(tx.execute(any())).thenAnswer(inv ->
                inv.<org.springframework.transaction.support.TransactionCallback<?>>getArgument(0).doInTransaction(null));
        properties.getRetention().setBatchSize(2);
        service = new TaskRetentionService(repository, properties, tx, metrics);
    }

    @Test void purgesFinishedTasksInBatchesUntilAShortBatchRemovingReferencesFirst() {
        List<UUID> full = IntStream.range(0, 2).mapToObj(i -> UUID.randomUUID()).toList();
        List<UUID> partial = List.of(UUID.randomUUID());
        when(repository.findPurgeableTaskIds(any(), eq(2))).thenReturn(full).thenReturn(partial);
        when(repository.deleteTasksByIds(full)).thenReturn(2);
        when(repository.deleteTasksByIds(partial)).thenReturn(1);

        service.purgeExpiredTasks();

        var order = inOrder(repository);
        order.verify(repository).deleteIdempotencyRecords(full);
        order.verify(repository).clearReplaySource(full);
        order.verify(repository).deleteTasksByIds(full);
        order.verify(repository).deleteTasksByIds(partial);
        verify(repository, times(2)).findPurgeableTaskIds(any(), eq(2));
        verify(metrics).recordTasksPurged(3);
    }

    @Test void usesConfiguredRetentionWindow() {
        properties.getRetention().setRetentionHours(24);
        when(repository.findPurgeableTaskIds(any(), anyInt())).thenReturn(List.of());
        Instant before = Instant.now();

        service.purgeExpiredTasks();

        var cutoff = org.mockito.ArgumentCaptor.forClass(Instant.class);
        verify(repository).findPurgeableTaskIds(cutoff.capture(), anyInt());
        org.junit.jupiter.api.Assertions.assertTrue(
                !cutoff.getValue().isAfter(before.minusSeconds(24 * 3600 - 5))
                        && cutoff.getValue().isAfter(before.minusSeconds(24 * 3600 + 60)));
    }

    @Test void doesNothingWhenDisabled() {
        properties.getRetention().setEnabled(false);
        service.purgeExpiredTasks();
        verifyNoInteractions(repository);
    }

    @Test void swallowsRepositoryFailures() {
        when(repository.findPurgeableTaskIds(any(), anyInt())).thenThrow(new IllegalStateException("db down"));
        service.purgeExpiredTasks();
        verify(repository, never()).deleteTasksByIds(any());
    }
}
