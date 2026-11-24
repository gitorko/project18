package com.taskprocessor.entity;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/** Stores named task priorities as stable numeric ranks for efficient SQL ordering. */
@Converter
public class TaskPriorityConverter implements AttributeConverter<TaskPriority, Integer> {

    @Override
    public Integer convertToDatabaseColumn(TaskPriority priority) {
        return priority == null ? TaskPriority.MEDIUM.getDatabaseValue() : priority.getDatabaseValue();
    }

    @Override
    public TaskPriority convertToEntityAttribute(Integer value) {
        return TaskPriority.fromDatabaseValue(value);
    }
}
