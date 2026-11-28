package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Purges finished (COMPLETED, FAILED, CANCELLED) tasks once they pass the retention window,
 * so the table, its indexes and the summary queries do not grow without bound. A purged
 * FAILED task can no longer be inspected or replayed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskRetentionService {

    /** Upper bound on batches per run so one run cannot monopolise the database. */
    private static final int MAX_BATCHES_PER_RUN = 100;

    private final TaskRepository taskRepository;
    private final TaskProcessorProperties properties;
    private final TransactionTemplate transactionTemplate;

    @Scheduled(fixedDelayString = "${task-processor.retention.purge-interval-ms:300000}",
            initialDelayString = "${task-processor.retention.purge-interval-ms:300000}")
    public void purgeExpiredTasks() {
        var retention = properties.getRetention();
        if (!retention.isEnabled()) {
            return;
        }
        try {
            Instant cutoff = Instant.now().minus(Duration.ofHours(retention.getRetentionHours()));
            int batchSize = Math.max(1, retention.getBatchSize());
            long total = 0;
            for (int batch = 0; batch < MAX_BATCHES_PER_RUN; batch++) {
                Integer purged = transactionTemplate.execute(status -> purgeBatch(cutoff, batchSize));
                int count = purged == null ? 0 : purged;
                total += count;
                if (count < batchSize) {
                    break;
                }
            }
            if (total > 0) {
                log.info("Purged {} finished tasks older than {}", total, cutoff);
            }
        } catch (Exception e) {
            log.error("Error during task retention purge: {}", e.getMessage(), e);
        }
    }

    private int purgeBatch(Instant cutoff, int batchSize) {
        List<UUID> ids = taskRepository.findPurgeableTaskIds(cutoff, batchSize);
        if (ids.isEmpty()) {
            return 0;
        }
        // Remove rows that reference these tasks before the tasks themselves.
        taskRepository.deleteIdempotencyRecords(ids);
        taskRepository.clearReplaySource(ids);
        return taskRepository.deleteTasksByIds(ids);
    }
}
