package com.taskprocessor.repository;

import com.taskprocessor.entity.TaskSchedule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import jakarta.persistence.LockModeType;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskScheduleRepository extends JpaRepository<TaskSchedule, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM TaskSchedule s WHERE s.id = :scheduleId")
    java.util.Optional<TaskSchedule> findByIdForUpdate(@Param("scheduleId") UUID scheduleId);

    @Query(value = """
        SELECT * FROM task_schedules
        WHERE enabled = true AND active_task_id IS NULL AND next_run_at <= :now
        ORDER BY next_run_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<TaskSchedule> findDueSchedulesForUpdate(@Param("now") Instant now, @Param("limit") int limit);

    /** Clear occurrences that became terminal outside the normal task finalization path. */
    @Modifying
    @Query(value = """
        UPDATE task_schedules s
        SET active_task_id = NULL,
            next_run_at = CASE WHEN s.enabled
                THEN COALESCE((SELECT t.completed_at FROM tasks t WHERE t.id = s.active_task_id), :now)
                     + (s.interval_seconds * interval '1 second')
                ELSE NULL END,
            updated_at = :now
        WHERE s.active_task_id IN (
            SELECT t.id FROM tasks t WHERE t.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
        )
        """, nativeQuery = true)
    int releaseTerminalOccurrences(@Param("now") Instant now);
}
