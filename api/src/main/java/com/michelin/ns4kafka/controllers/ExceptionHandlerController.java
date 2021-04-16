package com.michelin.ns4kafka.controllers;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.hateoas.JsonError;

@Controller("/errors")
public class ExceptionHandlerController {

    @Error(global = true)
    public HttpResponse<JsonError> error(HttpRequest<?> request, ResourceValidationException exception) {
        String message = exception.getValidationErrors().toString();
        return HttpResponse.badRequest()
                .body(new JsonError("Validation failed: " + message));
    }

    @Error(global = true)
    public HttpResponse<JsonError> error(HttpRequest<?> request, ResourceNotFoundException exception) {
        return HttpResponse.notFound().body(new JsonError("Ressource not Found"));
    }
}
