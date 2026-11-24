package com.taskprocessor.repository;

import com.taskprocessor.entity.TaskIdempotencyRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface TaskIdempotencyRepository extends JpaRepository<TaskIdempotencyRecord, String> {
    @Modifying
    @Query(value = """
        INSERT INTO task_idempotency (idempotency_key, request_hash, task_id)
        VALUES (:key, :requestHash, :taskId)
        ON CONFLICT (idempotency_key) DO NOTHING
        """, nativeQuery = true)
    int reserve(@Param("key") String key, @Param("requestHash") String requestHash,
                @Param("taskId") UUID taskId);
}
