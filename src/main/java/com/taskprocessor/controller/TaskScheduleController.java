package com.taskprocessor.controller;

import com.taskprocessor.dto.CreateTaskScheduleRequest;
import com.taskprocessor.dto.TaskScheduleResponse;
import com.taskprocessor.service.ScheduledTaskService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/schedules")
@RequiredArgsConstructor
public class TaskScheduleController {

    private final ScheduledTaskService scheduledTaskService;

    @PostMapping
    public ResponseEntity<TaskScheduleResponse> createSchedule(
            @Valid @RequestBody CreateTaskScheduleRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(scheduledTaskService.createSchedule(request));
    }

    @GetMapping("/{scheduleId}")
    public ResponseEntity<TaskScheduleResponse> getSchedule(@PathVariable UUID scheduleId) {
        return scheduledTaskService.getSchedule(scheduleId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{scheduleId}")
    public ResponseEntity<Void> cancelSchedule(@PathVariable UUID scheduleId) {
        return scheduledTaskService.cancelSchedule(scheduleId)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }
}
