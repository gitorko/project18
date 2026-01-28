package com.taskprocessor.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BulkCreateTaskRequest {

    @NotEmpty(message = "Tasks list cannot be empty")
    @Size(max = 1000, message = "Cannot create more than 1000 tasks at once")
    @Valid
    private List<CreateTaskRequest> tasks;
}
