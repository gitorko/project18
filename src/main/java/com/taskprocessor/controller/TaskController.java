package com.taskprocessor.controller;

import com.taskprocessor.dto.*;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.service.TaskQueueService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST API for task queue operations.
 */
@RestController
@RequestMapping("/api/v1/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskQueueService taskQueueService;

    /**
     * Enqueue a single task
     */
    @PostMapping
    public ResponseEntity<TaskResponse> createTask(@Valid @RequestBody CreateTaskRequest request) {
        TaskResponse task = taskQueueService.enqueueTask(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(task);
    }

    /**
     * Enqueue multiple tasks in a batch
     */
    @PostMapping("/bulk")
    public ResponseEntity<List<TaskResponse>> createTasks(@Valid @RequestBody BulkCreateTaskRequest request) {
        List<TaskResponse> tasks = taskQueueService.enqueueTasks(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(tasks);
    }

    /**
     * Get task by ID
     */
    @GetMapping("/{taskId}")
    public ResponseEntity<TaskResponse> getTask(@PathVariable UUID taskId) {
        return taskQueueService.getTask(taskId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get paginated list of tasks
     */
    @GetMapping
    public ResponseEntity<Page<TaskResponse>> getTasks(
            @RequestParam(required = false) TaskStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        
        Pageable pageable = PageRequest.of(page, Math.min(size, 100));
        Page<TaskResponse> tasks = taskQueueService.getTasks(status, pageable);
        return ResponseEntity.ok(tasks);
    }

    /**
     * Get task summary statistics
     */
    @GetMapping("/summary")
    public ResponseEntity<TaskSummaryResponse> getTaskSummary() {
        TaskSummaryResponse summary = taskQueueService.getTaskSummary();
        return ResponseEntity.ok(summary);
    }

    /**
     * Cancel a pending task
     */
    @DeleteMapping("/{taskId}")
    public ResponseEntity<Void> cancelTask(@PathVariable UUID taskId) {
        boolean cancelled = taskQueueService.cancelTask(taskId);
        if (cancelled) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.notFound().build();
    }

    /**
     * Get tasks currently being processed by this worker
     */
    @GetMapping("/processing")
    public ResponseEntity<List<TaskResponse>> getProcessingTasks() {
        List<TaskResponse> tasks = taskQueueService.getProcessingTasks();
        return ResponseEntity.ok(tasks);
    }
}
