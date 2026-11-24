package com.taskprocessor.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskprocessor.entity.TaskPriority;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@JsonTest
class TaskPriorityJsonTest {
    @Autowired
    private ObjectMapper objectMapper;

    @Test void readsAndWritesNamedPriorityValues() throws Exception {
        assertEquals(TaskPriority.HIGH, objectMapper.readValue("\"HIGH\"", TaskPriority.class));
        assertEquals("\"LOW\"", objectMapper.writeValueAsString(TaskPriority.LOW));
    }

    @Test void rejectsNumericPriorityValues() {
        assertThrows(JsonProcessingException.class,
                () -> objectMapper.readValue("2", TaskPriority.class));
    }
}
