package com.taskprocessor.service;

public class TaskNotReplayableException extends RuntimeException {
    public TaskNotReplayableException(String message) {
        super(message);
    }
}
