package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Service responsible for recovering stale tasks.
 * 
 * Tasks are considered stale when:
 * 1. They are in PROCESSING status
 * 2. Their last heartbeat is older than the configured threshold
 * 
 * This handles scenarios where:
 * - A worker crashes while processing a task
 * - A worker loses network connectivity
 * - A worker is forcefully terminated
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StaleTaskRecoveryService {

    private final TaskRepository taskRepository;
    private final TaskProcessorProperties properties;

    /**
     * Periodically check for and recover stale tasks.
     * Uses a scheduled task instead of continuous polling to reduce DB load.
     */
    @Scheduled(fixedDelayString = "${task-processor.heartbeat.recovery-interval-ms:10000}")
    @Transactional
    public void recoverStaleTasks() {
        try {
            Instant staleThreshold = Instant.now()
                    .minusMillis(properties.getHeartbeat().getStaleThresholdMs());
            Instant now = Instant.now();

            // First, reset tasks that can be retried
            int resetCount = taskRepository.resetStaleTasks(staleThreshold, now);
            if (resetCount > 0) {
                log.info("Reset {} stale tasks for retry", resetCount);
            }

            // Then, mark exhausted tasks as failed
            int failedCount = taskRepository.markExhaustedTasksAsFailed(staleThreshold, now);
            if (failedCount > 0) {
                log.warn("Marked {} tasks as failed (max retries exceeded)", failedCount);
            }

        } catch (Exception e) {
            log.error("Error during stale task recovery: {}", e.getMessage(), e);
        }
    }
}
