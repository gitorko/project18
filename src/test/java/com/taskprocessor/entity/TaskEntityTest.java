package com.taskprocessor.entity;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TaskEntityTest {

    @Test
    void createsIdWhenMissingAndPreservesPreassignedId() {
        Task generated = Task.builder().taskType("mail").build();
        generated.onCreate();
        assertNotNull(generated.getId());

        UUID reservedId = UUID.randomUUID();
        Task assigned = Task.builder().id(reservedId).taskType("mail").build();
        assigned.onCreate();
        assertEquals(reservedId, assigned.getId());
    }

    @org.junit.jupiter.api.Test void newTasksAreNewUntilPersistedOrLoaded() {
        Task task = Task.builder().id(java.util.UUID.randomUUID()).taskType("t").build();
        org.junit.jupiter.api.Assertions.assertTrue(task.isNew());
        task.markNotNew();
        org.junit.jupiter.api.Assertions.assertFalse(task.isNew());
    }
}
