package com.michelin.ns4kafka.controllers;

import java.util.List;

import javax.inject.Singleton;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.hateoas.JsonError;
import lombok.Getter;

@Singleton
public class ExceptionHandlerController {

    @Error(global = true)
    public HttpResponse<ResourceCreationError> error(HttpRequest<?> request, ResourceValidationException exception) {
        return HttpResponse.badRequest()
                .body(new ResourceCreationError("Message validation failed", exception.getValidationErrors()));
    }

    @Error(global = true)
    public HttpResponse<JsonError> error(HttpRequest<?> request, ResourceNotFoundException exception) {
        return HttpResponse.notFound().body(new JsonError("Ressource not Found"));
    }
}

class ResourceCreationError extends JsonError {

    @Getter
    private List<String> validationErrors;

    public ResourceCreationError(String message, List<String> validationErrors) {
        super(message);
        this.validationErrors = validationErrors;
    }
}
