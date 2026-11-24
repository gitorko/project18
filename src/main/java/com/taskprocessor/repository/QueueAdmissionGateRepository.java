package com.taskprocessor.repository;

import com.taskprocessor.entity.QueueAdmissionGate;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;

public interface QueueAdmissionGateRepository extends JpaRepository<QueueAdmissionGate, Integer> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT g FROM QueueAdmissionGate g WHERE g.id = 1")
    Optional<QueueAdmissionGate> lockAdmissionGate();
}
