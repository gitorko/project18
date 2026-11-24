package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.repository.QueueAdmissionGateRepository;
import com.taskprocessor.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Enforces queue limits while serializing admission decisions across app instances. */
@Service
@RequiredArgsConstructor
public class QueueAdmissionService {
    private static final Collection<TaskStatus> OUTSTANDING_STATUSES =
            EnumSet.of(TaskStatus.PENDING, TaskStatus.PROCESSING, TaskStatus.SCHEDULED);

    private final TaskRepository taskRepository;
    private final QueueAdmissionGateRepository gateRepository;
    private final TaskProcessorProperties properties;

    @Transactional(propagation = Propagation.MANDATORY)
    public void assertCanAccept(List<Task> newTasks) {
        if (!canAccept(newTasks)) {
            throw new QueueCapacityExceededException("Task queue capacity has been reached");
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean canAccept(List<Task> newTasks) {
        var limits = properties.getAdmission();
        if (limits.getMaxOutstandingTasks() <= 0 && limits.getMaxOutstandingByType().isEmpty()) {
            return true;
        }

        gateRepository.lockAdmissionGate()
                .orElseThrow(() -> new IllegalStateException("Queue admission gate row is missing"));

        long currentTotal = taskRepository.countByStatusIn(OUTSTANDING_STATUSES);
        if (limits.getMaxOutstandingTasks() > 0
                && currentTotal + newTasks.size() > limits.getMaxOutstandingTasks()) {
            return false;
        }

        Map<String, Long> incomingByType = new HashMap<>();
        newTasks.forEach(task -> incomingByType.merge(task.getTaskType(), 1L, Long::sum));
        for (var incoming : incomingByType.entrySet()) {
            Long limit = limits.getMaxOutstandingByType().get(incoming.getKey());
            if (limit != null && limit > 0) {
                long current = taskRepository.countByTaskTypeAndStatusIn(incoming.getKey(), OUTSTANDING_STATUSES);
                if (current + incoming.getValue() > limit) {
                    return false;
                }
            }
        }
        return true;
    }
}
