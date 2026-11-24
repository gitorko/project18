package com.taskprocessor.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Singleton row used to serialize queue admission decisions across worker nodes. */
@Entity
@Table(name = "queue_admission_gate")
@Getter
@NoArgsConstructor
public class QueueAdmissionGate {
    @Id
    @Column(name = "id")
    private Integer id;
}
