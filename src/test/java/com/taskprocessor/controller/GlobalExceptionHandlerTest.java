package com.taskprocessor.controller;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;

import static org.junit.jupiter.api.Assertions.*;

class GlobalExceptionHandlerTest {
    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test void mapsValidationFailuresToBadRequest() {
        Object target = new Object();
        BeanPropertyBindingResult binding = new BeanPropertyBindingResult(target, "request");
        binding.addError(new FieldError("request", "taskType", "Task type is required"));
        var response = handler.handleValidationException(new MethodArgumentNotValidException(null, binding));
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals("Validation failed", response.getBody().getError());
        assertTrue(response.getBody().getMessage().contains("taskType"));
    }

    @Test void mapsIllegalArgumentToBadRequest() {
        var response = handler.handleIllegalArgument(new IllegalArgumentException("bad input"));
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals("Invalid argument", response.getBody().getError());
        assertEquals("bad input", response.getBody().getMessage());
        assertNotNull(response.getBody().getTimestamp());
    }

    @Test void mapsUnexpectedExceptionToGenericInternalError() {
        var response = handler.handleGenericException(new IllegalStateException("secret detail"));
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertEquals("An unexpected error occurred", response.getBody().getMessage());
    }
}
