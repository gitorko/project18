package com.taskprocessor.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Task entity representing a unit of work in the queue.
 * Uses PostgreSQL-specific features for efficient queue operations.
 */
@Entity
@Table(name = "tasks", indexes = {
    @Index(name = "idx_tasks_status_priority_created", columnList = "status, priority DESC, created_at ASC"),
    @Index(name = "idx_tasks_status", columnList = "status"),
    @Index(name = "idx_tasks_worker_id", columnList = "worker_id"),
    @Index(name = "idx_tasks_heartbeat", columnList = "last_heartbeat")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /**
     * Type of task - used to route to appropriate handler
     */
    @Column(name = "task_type", nullable = false, length = 255)
    private String taskType;

    /**
     * JSON payload containing task-specific data
     */
    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    /**
     * Current status of the task
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    @Builder.Default
    private TaskStatus status = TaskStatus.PENDING;

    /**
     * Priority for processing (higher = more urgent)
     */
    @Column(name = "priority")
    @Builder.Default
    private Integer priority = 0;

    /**
     * Number of times this task has been attempted
     */
    @Column(name = "attempt_count")
    @Builder.Default
    private Integer attemptCount = 0;

    /**
     * Maximum number of retry attempts
     */
    @Column(name = "max_attempts")
    @Builder.Default
    private Integer maxAttempts = 3;

    /**
     * ID of the worker/pod currently processing this task
     */
    @Column(name = "worker_id", length = 255)
    private String workerId;

    /**
     * Last heartbeat from the worker processing this task
     */
    @Column(name = "last_heartbeat")
    private Instant lastHeartbeat;

    /**
     * When the task was created
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /**
     * When the task was last updated
     */
    @Column(name = "updated_at")
    private Instant updatedAt;

    /**
     * When processing started
     */
    @Column(name = "started_at")
    private Instant startedAt;

    /**
     * When processing completed
     */
    @Column(name = "completed_at")
    private Instant completedAt;

    /**
     * Error message if task failed
     */
    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    /**
     * Scheduled time for delayed execution
     */
    @Column(name = "scheduled_at")
    private Instant scheduledAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
        if (scheduledAt == null) {
            scheduledAt = createdAt;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }
}
