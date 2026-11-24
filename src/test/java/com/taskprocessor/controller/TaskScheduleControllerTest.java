package com.taskprocessor.controller;

import com.taskprocessor.dto.CreateTaskScheduleRequest;
import com.taskprocessor.dto.TaskScheduleResponse;
import com.taskprocessor.service.ScheduledTaskService;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class TaskScheduleControllerTest {
    private final ScheduledTaskService service = mock(ScheduledTaskService.class);
    private final TaskScheduleController controller = new TaskScheduleController(service);

    @Test void createsAndGetsSchedule() {
        UUID id = UUID.randomUUID();
        TaskScheduleResponse schedule = TaskScheduleResponse.builder().id(id).build();
        when(service.createSchedule(any())).thenReturn(schedule);
        assertEquals(201, controller.createSchedule(CreateTaskScheduleRequest.builder().build())
                .getStatusCode().value());

        when(service.getSchedule(id)).thenReturn(Optional.of(schedule));
        assertEquals(200, controller.getSchedule(id).getStatusCode().value());
        when(service.getSchedule(any())).thenReturn(Optional.empty());
        assertEquals(404, controller.getSchedule(UUID.randomUUID()).getStatusCode().value());
    }

    @Test void cancelsExistingScheduleAndReturnsNotFoundForUnknownSchedule() {
        UUID id = UUID.randomUUID();
        when(service.cancelSchedule(id)).thenReturn(true);
        assertEquals(204, controller.cancelSchedule(id).getStatusCode().value());
        when(service.cancelSchedule(any())).thenReturn(false);
        assertEquals(404, controller.cancelSchedule(UUID.randomUUID()).getStatusCode().value());
    }
}
