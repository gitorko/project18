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

    /** Lightweight view of a runnable task used to pick a batch without loading payloads. */
    interface CandidateRow {
        UUID getId();

        String getTaskType();

        /** Numeric priority rank as stored in the database. */
        Integer getPriority();
    }

    /**
     * Pick candidate tasks for processing using SKIP LOCKED to prevent multiple workers
     * from picking the same task. This is the key to distributed queue behavior.
     *
     * Only the columns needed for fair selection are read (no payload or other columns),
     * because the pool is deliberately larger than the batch and most rows are not claimed.
     * The rows stay locked until the surrounding transaction ends, so the selected ones can
     * then be claimed with {@link #claimTasks} without another worker interfering.
     *
     * The result is an over-fetched candidate pool (larger than the actual batch size)
     * so that TaskQueueService can apply fair round-robin selection by task type within
     * each priority tier, rather than the raw FIFO order returned here.
     */
    @Query(value = """
        SELECT id AS id, task_type AS taskType, priority AS priority FROM tasks
        WHERE status = 'PENDING'
        AND scheduled_at <= :now
        ORDER BY priority DESC, created_at ASC
        LIMIT :candidatePoolSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<CandidateRow> findCandidates(@Param("now") Instant now, @Param("candidatePoolSize") int candidatePoolSize);

    /**
     * Same as {@link #findCandidates} but skips task types that have no remaining
     * capacity on this worker, so a saturated type cannot fill the whole candidate pool
     * and starve runnable work of other types. {@code excludedTypes} must not be empty.
     */
    @Query(value = """
        SELECT id AS id, task_type AS taskType, priority AS priority FROM tasks
        WHERE status = 'PENDING'
        AND scheduled_at <= :now
        AND task_type NOT IN (:excludedTypes)
        ORDER BY priority DESC, created_at ASC
        LIMIT :candidatePoolSize
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<CandidateRow> findCandidatesExcludingTypes(@Param("now") Instant now,
                                                    @Param("candidatePoolSize") int candidatePoolSize,
                                                    @Param("excludedTypes") Collection<String> excludedTypes);

    /**
     * Claim the selected tasks for this worker in one statement and return the claimed rows
     * (including payloads), so only tasks that will actually run are ever loaded in full.
     */
    @Query(value = """
        UPDATE tasks
        SET status = 'PROCESSING',
            worker_id = :workerId,
            started_at = :now,
            last_heartbeat = :now,
            attempt_count = attempt_count + 1,
            updated_at = :now
        WHERE id IN (:ids) AND status = 'PENDING'
        RETURNING *
        """, nativeQuery = true)
    List<Task> claimTasks(@Param("ids") Collection<UUID> ids, @Param("workerId") String workerId,
                          @Param("now") Instant now);

    /**
     * Finalization updates. Each is guarded on the task still being PROCESSING for the given
     * attempt, so a late result from an older attempt (or one racing the timeout watchdog)
     * cannot overwrite newer state. They return the number of rows changed (0 or 1).
     */
    @Modifying
    @Query("""
        UPDATE Task t
        SET t.status = 'COMPLETED', t.completedAt = :now, t.workerId = null, t.updatedAt = :now
        WHERE t.id = :taskId AND t.status = 'PROCESSING' AND t.attemptCount = :attempt
        """)
    int markCompleted(@Param("taskId") UUID taskId, @Param("attempt") int attempt, @Param("now") Instant now);

    @Modifying
    @Query("""
        UPDATE Task t
        SET t.status = 'SCHEDULED', t.scheduledAt = :retryAt, t.workerId = null, t.lastHeartbeat = null,
            t.errorMessage = :error, t.updatedAt = :now
        WHERE t.id = :taskId AND t.status = 'PROCESSING' AND t.attemptCount = :attempt
        """)
    int scheduleRetry(@Param("taskId") UUID taskId, @Param("attempt") int attempt,
                      @Param("retryAt") Instant retryAt, @Param("error") String error, @Param("now") Instant now);

    @Modifying
    @Query("""
        UPDATE Task t
        SET t.status = 'FAILED', t.completedAt = :now, t.workerId = null,
            t.errorMessage = :error, t.updatedAt = :now
        WHERE t.id = :taskId AND t.status = 'PROCESSING' AND t.attemptCount = :attempt
        """)
    int markFailed(@Param("taskId") UUID taskId, @Param("attempt") int attempt,
                   @Param("error") String error, @Param("now") Instant now);

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
