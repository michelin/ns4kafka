package com.michelin.ns4kafka.controllers;

import java.util.stream.Collectors;

import com.michelin.ns4kafka.models.Status;
import com.michelin.ns4kafka.models.Status.StatusCause;
import com.michelin.ns4kafka.models.Status.StatusDetails;
import com.michelin.ns4kafka.models.Status.StatusPhase;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.hateoas.JsonError;

@Controller("/errors")
public class ExceptionHandlerController {

    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ResourceValidationException exception) {
        var causes = exception.getValidationErrors().stream()
            .map(validationError -> (StatusCause.builder()
                .message(validationError)
                .build()))
            .collect(Collectors.toList());

        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(String.format("Invalid %s %s", exception.getKind(), exception.getName()))
            .reason("Invalid")
            .details(StatusDetails.builder()
                .kind(exception.getKind())
                .name(exception.getName())
                .causes(causes)
                .build())
            .code(HttpResponse.unprocessableEntity().code())
            .build();

        return HttpResponse.unprocessableEntity()
                .body(status);
    }

    @Error(global = true)
    public HttpResponse<JsonError> error(HttpRequest<?> request, ResourceNotFoundException exception) {
        return HttpResponse.notFound().body(new JsonError("Ressource not Found"));
    }
}
