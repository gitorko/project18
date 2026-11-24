package com.taskprocessor.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskPriorityConverterTest {
    private final TaskPriorityConverter converter = new TaskPriorityConverter();

    @Test void convertsNamedPrioritiesToStableDatabaseRanksAndBack() {
        for (TaskPriority priority : TaskPriority.values()) {
            assertEquals(priority.getDatabaseValue(), converter.convertToDatabaseColumn(priority));
            assertEquals(priority, converter.convertToEntityAttribute(priority.getDatabaseValue()));
        }
    }

    @Test void defaultsNullToNormalAndRejectsUnknownDatabaseRanks() {
        assertEquals(TaskPriority.MEDIUM.getDatabaseValue(), converter.convertToDatabaseColumn(null));
        assertEquals(TaskPriority.MEDIUM, converter.convertToEntityAttribute(null));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToEntityAttribute(99));
    }
}
