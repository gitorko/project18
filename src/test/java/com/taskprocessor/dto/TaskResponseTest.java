package com.taskprocessor.dto;

import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskPriority;
import com.taskprocessor.entity.TaskStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TaskResponseTest {
    @Test void mapsTaskFieldsToResponse() {
        Instant now = Instant.now();
        UUID id = UUID.randomUUID();
        Task task = Task.builder().id(id).taskType("image").payload("{}").status(TaskStatus.PROCESSING)
                .priority(TaskPriority.HIGH).attemptCount(2).maxAttempts(3).workerId("worker")
                .createdAt(now).updatedAt(now).startedAt(now).completedAt(null).scheduledAt(now)
                .errorMessage("previous error").timeoutSeconds(40).build();
        TaskResponse response = TaskResponse.fromEntity(task);
        assertEquals(id, response.getId());
        assertEquals("image", response.getTaskType());
        assertEquals("{}", response.getPayload());
        assertEquals(TaskStatus.PROCESSING, response.getStatus());
        assertEquals(TaskPriority.HIGH, response.getPriority());
        assertEquals(2, response.getAttemptCount());
        assertEquals(3, response.getMaxAttempts());
        assertEquals("worker", response.getWorkerId());
        assertEquals(now, response.getCreatedAt());
        assertEquals("previous error", response.getErrorMessage());
        assertEquals(40, response.getTimeoutSeconds());
    }
}
