package com.michelin.ns4kafka.exceptionHandler;

import java.util.List;

import io.micronaut.http.hateoas.JsonError;
import lombok.Getter;

public class ResourceCreationError extends JsonError {
    @Getter
    List<String> validationErrors;

    public ResourceCreationError(String message, List<String> validationErrors) {
        super(message);
        this.validationErrors = validationErrors;
    }
}
