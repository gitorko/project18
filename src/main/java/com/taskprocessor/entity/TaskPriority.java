package com.taskprocessor.entity;

/** Named priority levels for queued tasks. Higher levels are selected first. */
public enum TaskPriority {
    LOW(-1),
    MEDIUM(0),
    HIGH(1);

    private final int databaseValue;

    TaskPriority(int databaseValue) {
        this.databaseValue = databaseValue;
    }

    public int getDatabaseValue() {
        return databaseValue;
    }

    public static TaskPriority fromDatabaseValue(Integer value) {
        if (value == null) {
            return MEDIUM;
        }
        for (TaskPriority priority : values()) {
            if (priority.databaseValue == value) {
                return priority;
            }
        }
        throw new IllegalArgumentException("Unknown task priority database value: " + value);
    }
}
