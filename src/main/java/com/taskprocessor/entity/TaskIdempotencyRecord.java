package com.taskprocessor.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.AllArgsConstructor;

import java.util.UUID;

@Entity
@Table(name = "task_idempotency")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TaskIdempotencyRecord {
    @Id
    @Column(name = "idempotency_key", length = 255)
    private String idempotencyKey;

    @Column(name = "request_hash", nullable = false, length = 64)
    private String requestHash;

    @Column(name = "task_id", nullable = false)
    private UUID taskId;
}
