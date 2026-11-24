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
}
