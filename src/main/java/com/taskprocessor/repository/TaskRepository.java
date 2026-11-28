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
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.Lock;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.Collection;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {

    /** Lock a task while finalizing an attempt so a stale worker cannot overwrite a retry. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT t FROM Task t WHERE t.id = :taskId")
    Optional<Task> findByIdForUpdate(@Param("taskId") UUID taskId);

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
     * Same as {@link #findTasksForProcessing} but skips task types that have no remaining
     * capacity on this worker, so a saturated type cannot fill the whole candidate pool
     * and starve runnable work of other types. {@code excludedTypes} must not be empty.
     */
    @Query(value = """
        SELECT * FROM tasks
        WHERE status = 'PENDING'
        AND scheduled_at <= :now
        AND task_type NOT IN (:excludedTypes)
        ORDER BY priority DESC, created_at ASC
        LIMIT :candidatePoolSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<Task> findTasksForProcessingExcludingTypes(@Param("now") Instant now,
                                                    @Param("candidatePoolSize") int candidatePoolSize,
                                                    @Param("excludedTypes") Collection<String> excludedTypes);

    /** Promote due scheduled tasks and delayed retries, distributing rows safely across workers. */
    @Modifying
    @Query(value = """
        WITH due_tasks AS (
            SELECT id FROM tasks
            WHERE status = 'SCHEDULED' AND scheduled_at <= :now
            ORDER BY scheduled_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks t
        SET status = 'PENDING', updated_at = :now
        FROM due_tasks d
        WHERE t.id = d.id
        """, nativeQuery = true)
    int promoteDueScheduledTasks(@Param("now") Instant now, @Param("limit") int limit);

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

    long countByStatusIn(Collection<TaskStatus> statuses);

    long countByTaskTypeAndStatusIn(String taskType, Collection<TaskStatus> statuses);

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

    /** Update heartbeats for many tasks in a single round trip. */
    @Modifying
    @Query("UPDATE Task t SET t.lastHeartbeat = :heartbeat "
            + "WHERE t.id IN :taskIds AND t.workerId = :workerId AND t.status = 'PROCESSING'")
    int updateHeartbeats(@Param("taskIds") Collection<UUID> taskIds, @Param("workerId") String workerId,
                         @Param("heartbeat") Instant heartbeat);

    /**
     * Hand tasks that this worker was still running at shutdown straight back to the queue.
     * The aborted attempt is not counted against the retry budget.
     */
    @Modifying
    @Query("""
        UPDATE Task t
        SET t.status = 'PENDING',
            t.workerId = null,
            t.lastHeartbeat = null,
            t.attemptCount = t.attemptCount - 1,
            t.updatedAt = :now
        WHERE t.id IN :taskIds
        AND t.status = 'PROCESSING'
        AND t.workerId = :workerId
        AND t.attemptCount > 0
        """)
    int releaseProcessingTasks(@Param("taskIds") Collection<UUID> taskIds, @Param("workerId") String workerId,
                               @Param("now") Instant now);

    /** Select finished (COMPLETED, FAILED, CANCELLED) tasks old enough to purge, locking them so concurrent purgers do not collide. */
    @Query(value = """
        SELECT id FROM tasks
        WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
        AND updated_at < :cutoff
        AND NOT EXISTS (SELECT 1 FROM task_schedules s WHERE s.active_task_id = tasks.id)
        ORDER BY updated_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<UUID> findPurgeableTaskIds(@Param("cutoff") Instant cutoff, @Param("limit") int limit);

    /** Detach replay tasks from sources that are about to be purged. */
    @Modifying
    @Query(value = "UPDATE tasks SET replay_of_task_id = NULL WHERE replay_of_task_id IN (:ids)",
            nativeQuery = true)
    int clearReplaySource(@Param("ids") Collection<UUID> ids);

    /** Remove idempotency reservations that point at tasks about to be purged. */
    @Modifying
    @Query(value = "DELETE FROM task_idempotency WHERE task_id IN (:ids)", nativeQuery = true)
    int deleteIdempotencyRecords(@Param("ids") Collection<UUID> ids);

    @Modifying
    @Query(value = "DELETE FROM tasks WHERE id IN (:ids)", nativeQuery = true)
    int deleteTasksByIds(@Param("ids") Collection<UUID> ids);

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

    /** Count tasks by type and lifecycle status for the public queue summary. */
    @Query("SELECT t.taskType, t.status, COUNT(t) FROM Task t GROUP BY t.taskType, t.status")
    List<Object[]> countByTaskTypeAndStatus();
}
