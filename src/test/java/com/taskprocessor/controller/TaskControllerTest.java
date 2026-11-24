package com.taskprocessor.controller;

import com.taskprocessor.dto.CreateTaskRequest;
import com.taskprocessor.dto.BulkCreateTaskRequest;
import com.taskprocessor.dto.TaskResponse;
import com.taskprocessor.dto.TaskSummaryResponse;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.service.TaskQueueService;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TaskControllerTest {
    private final TaskQueueService service = mock(TaskQueueService.class);
    private final TaskController controller = new TaskController(service);

    @Test void createsAndGetsTasks() {
        TaskResponse task = TaskResponse.builder().id(UUID.randomUUID()).build();
        when(service.enqueueTask(any(), eq("client-request-1"))).thenReturn(task);
        assertEquals(201, controller.createTask(CreateTaskRequest.builder().taskType("x").build(), "client-request-1")
                .getStatusCode().value());
        when(service.enqueueTasks(any())).thenReturn(List.of(task));
        BulkCreateTaskRequest bulk = BulkCreateTaskRequest.builder().tasks(List.of()).build();
        when(service.enqueueTasks(bulk)).thenReturn(List.of(task));
        assertEquals(201, controller.createTasks(bulk).getStatusCode().value());
        when(service.getTask(task.getId())).thenReturn(Optional.of(task));
        assertTrue(controller.getTask(task.getId()).hasBody());
        when(service.getTask(any())).thenReturn(Optional.empty());
        assertEquals(404, controller.getTask(UUID.randomUUID()).getStatusCode().value());
    }

    @Test void listsSummarizesCancelsAndReturnsProcessingTasks() {
        when(service.getTasks(eq(TaskStatus.PENDING), any())).thenReturn(new PageImpl<>(List.of()));
        controller.getTasks(TaskStatus.PENDING, 0, 200);
        verify(service).getTasks(eq(TaskStatus.PENDING), eq(PageRequest.of(0, 100)));
        when(service.getTaskSummary()).thenReturn(TaskSummaryResponse.builder().build());
        assertNotNull(controller.getTaskSummary().getBody());
        when(service.cancelTask(any())).thenReturn(true, false);
        UUID id = UUID.randomUUID();
        assertEquals(204, controller.cancelTask(id).getStatusCode().value());
        assertEquals(404, controller.cancelTask(id).getStatusCode().value());
        when(service.getProcessingTasks()).thenReturn(List.of());
        assertTrue(controller.getProcessingTasks().getBody().isEmpty());
    }

    @Test void replaysFailedTaskAndReturnsNotFoundForUnknownTask() {
        UUID id = UUID.randomUUID();
        TaskResponse replay = TaskResponse.builder().id(UUID.randomUUID()).build();
        when(service.replayFailedTask(id)).thenReturn(Optional.of(replay));
        assertEquals(201, controller.replayTask(id).getStatusCode().value());
        when(service.replayFailedTask(any())).thenReturn(Optional.empty());
        assertEquals(404, controller.replayTask(UUID.randomUUID()).getStatusCode().value());
    }
}
