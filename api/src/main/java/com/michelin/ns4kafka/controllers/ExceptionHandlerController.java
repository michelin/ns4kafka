package com.michelin.ns4kafka.controllers;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.hateoas.JsonError;
import lombok.Getter;

import java.util.List;

@Controller("/errors")
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
