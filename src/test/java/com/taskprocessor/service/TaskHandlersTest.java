package com.taskprocessor.service;

import com.taskprocessor.entity.Task;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskHandlersTest {
    @Test void interfaceProvidesDefaultEstimate() {
        TaskHandler handler = new TaskHandler() {
            public String getTaskType() { return "custom"; }
            public void handle(Task task) { }
        };
        assertEquals(1000, handler.getEstimatedDurationMs());
    }

    @Test void defaultHandlerAcceptsTaskAndReportsEstimate() throws Exception {
        DefaultTaskHandler handler = new DefaultTaskHandler();
        assertEquals(DefaultTaskHandler.DEFAULT_TASK_TYPE, handler.getTaskType());
        assertEquals(100, handler.getEstimatedDurationMs());
        assertDoesNotThrow(() -> handler.handle(Task.builder().taskType("default").build()));
    }

    @Test void sampleHandlerProcessesPayloadAndSurfacesConfiguredFailure() throws Exception {
        SampleTaskHandler handler = new SampleTaskHandler();
        assertEquals("sample", handler.getTaskType());
        assertEquals(500, handler.getEstimatedDurationMs());
        assertDoesNotThrow(() -> handler.handle(Task.builder().taskType("sample")
                .payload("{\"processingTimeMs\":0}").build()));
        assertThrows(RuntimeException.class, () -> handler.handle(Task.builder().taskType("sample")
                .payload("{\"processingTimeMs\":0,\"failRate\":1}").build()));
        assertDoesNotThrow(() -> handler.handle(Task.builder().taskType("sample").payload("invalid-json").build()));
    }
}
