package com.taskprocessor.service;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.entity.QueueAdmissionGate;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.repository.QueueAdmissionGateRepository;
import com.taskprocessor.repository.TaskRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class QueueAdmissionServiceTest {
    private final TaskRepository taskRepository = mock(TaskRepository.class);
    private final QueueAdmissionGateRepository gateRepository = mock(QueueAdmissionGateRepository.class);
    private final TaskProcessorProperties properties = new TaskProcessorProperties();
    private final QueueAdmissionService service = new QueueAdmissionService(taskRepository, gateRepository, properties);

    @Test void rejectsSubmissionWhenGlobalOutstandingLimitWouldBeExceeded() {
        when(gateRepository.lockAdmissionGate()).thenReturn(Optional.of(mock(QueueAdmissionGate.class)));
        when(taskRepository.countByStatusIn(anyCollection())).thenReturn(2L);
        properties.getAdmission().setMaxOutstandingTasks(2);

        assertFalse(service.canAccept(List.of(Task.builder().taskType("mail").build())));
        verify(gateRepository).lockAdmissionGate();
    }

    @Test void enforcesPerTypeLimitAndAllowsOtherTypes() {
        when(gateRepository.lockAdmissionGate()).thenReturn(Optional.of(mock(QueueAdmissionGate.class)));
        when(taskRepository.countByStatusIn(anyCollection())).thenReturn(0L);
        when(taskRepository.countByTaskTypeAndStatusIn(eq("reports"), anyCollection())).thenReturn(3L);
        properties.getAdmission().setMaxOutstandingTasks(0);
        properties.getAdmission().getMaxOutstandingByType().put("reports", 3L);

        assertFalse(service.canAccept(List.of(Task.builder().taskType("reports").build())));
        assertTrue(service.canAccept(List.of(Task.builder().taskType("email").build())));
    }

    @Test void skipsDatabaseLockWhenAdmissionLimitsAreDisabled() {
        properties.getAdmission().setMaxOutstandingTasks(0);
        properties.getAdmission().getMaxOutstandingByType().clear();
        assertTrue(service.canAccept(List.of(Task.builder().taskType("mail").build())));
        verifyNoInteractions(gateRepository, taskRepository);
    }

    @Test void assertCanAcceptRaisesCapacityError() {
        when(gateRepository.lockAdmissionGate()).thenReturn(Optional.of(mock(QueueAdmissionGate.class)));
        when(taskRepository.countByStatusIn(anyCollection())).thenReturn(5L);
        properties.getAdmission().setMaxOutstandingTasks(4);
        assertThrows(QueueCapacityExceededException.class,
                () -> service.assertCanAccept(List.of(Task.builder().taskType("mail").build())));
    }
}
