package com.taskprocessor.repository;

import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {

    /**
     * Fetch candidate tasks for processing using SKIP LOCKED to prevent multiple workers
     * from picking the same task. This is the key to distributed queue behavior.
     *
     * SKIP LOCKED skips rows that are already locked by other transactions,
     * ensuring no two workers process the same task.
     *
     * The result is an over-fetched candidate pool (larger than the actual batch size)
     * so that TaskQueueService can apply fair round-robin selection by task type within
     * each priority tier, rather than the raw FIFO order returned here.
     */
    @Query(value = """
        SELECT * FROM tasks
        WHERE status = 'PENDING'
        AND scheduled_at <= :now
        ORDER BY priority DESC, created_at ASC
        LIMIT :candidatePoolSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<Task> findTasksForProcessing(@Param("now") Instant now, @Param("candidatePoolSize") int candidatePoolSize);

    /**
     * Find stale tasks that have been processing for too long without heartbeat.
     * These are likely from crashed workers and need to be recovered.
     */
    @Query(value = """
        SELECT * FROM tasks 
        WHERE status = 'PROCESSING' 
        AND last_heartbeat < :staleThreshold
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<Task> findStaleTasks(@Param("staleThreshold") Instant staleThreshold);

    /**
     * Count tasks by status for summary
     */
    long countByStatus(TaskStatus status);

    /**
     * Find tasks by status with pagination
     */
    Page<Task> findByStatus(TaskStatus status, Pageable pageable);

    /**
     * Find all tasks with pagination
     */
    Page<Task> findAllByOrderByCreatedAtDesc(Pageable pageable);

    /**
     * Find tasks by status ordered by priority and creation time
     */
    Page<Task> findByStatusOrderByPriorityDescCreatedAtAsc(TaskStatus status, Pageable pageable);

    /**
     * Bulk update status for multiple tasks
     */
    @Modifying
    @Query("UPDATE Task t SET t.status = :status, t.updatedAt = :now WHERE t.id IN :ids")
    int bulkUpdateStatus(@Param("ids") List<UUID> ids, @Param("status") TaskStatus status, @Param("now") Instant now);

    /**
     * Reset stale tasks back to PENDING status
     */
    @Modifying
    @Query("""
        UPDATE Task t 
        SET t.status = 'PENDING', 
            t.workerId = null, 
            t.lastHeartbeat = null,
            t.attemptCount = t.attemptCount + 1,
            t.updatedAt = :now
        WHERE t.status = 'PROCESSING' 
        AND t.lastHeartbeat < :staleThreshold
        AND t.attemptCount < t.maxAttempts
        """)
    int resetStaleTasks(@Param("staleThreshold") Instant staleThreshold, @Param("now") Instant now);

    /**
     * Mark tasks as FAILED if they exceeded max attempts
     */
    @Modifying
    @Query("""
        UPDATE Task t 
        SET t.status = 'FAILED', 
            t.workerId = null,
            t.errorMessage = 'Max retry attempts exceeded',
            t.updatedAt = :now
        WHERE t.status = 'PROCESSING' 
        AND t.lastHeartbeat < :staleThreshold
        AND t.attemptCount >= t.maxAttempts
        """)
    int markExhaustedTasksAsFailed(@Param("staleThreshold") Instant staleThreshold, @Param("now") Instant now);

    /**
     * Update heartbeat for a specific task
     */
    @Modifying
    @Query("UPDATE Task t SET t.lastHeartbeat = :heartbeat WHERE t.id = :taskId AND t.workerId = :workerId")
    int updateHeartbeat(@Param("taskId") UUID taskId, @Param("workerId") String workerId, @Param("heartbeat") Instant heartbeat);

    /**
     * Find tasks currently being processed by a specific worker
     */
    List<Task> findByWorkerIdAndStatus(String workerId, TaskStatus status);

    /**
     * Count tasks by worker
     */
    @Query("SELECT COUNT(t) FROM Task t WHERE t.workerId = :workerId AND t.status = 'PROCESSING'")
    long countProcessingByWorker(@Param("workerId") String workerId);

    /**
     * Get task type distribution for pending tasks
     */
    @Query("SELECT t.taskType, COUNT(t) FROM Task t WHERE t.status = :status GROUP BY t.taskType")
    List<Object[]> countByTaskTypeAndStatus(@Param("status") TaskStatus status);
}
