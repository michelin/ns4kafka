package com.michelin.ns4kafka.controllers;

import java.util.List;
import java.util.stream.Collectors;

import com.michelin.ns4kafka.models.Status;
import com.michelin.ns4kafka.models.Status.StatusCause;
import com.michelin.ns4kafka.models.Status.StatusDetails;
import com.michelin.ns4kafka.models.Status.StatusPhase;
import com.michelin.ns4kafka.services.ResourceInternalErrorException;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
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
            .code(HttpStatus.UNPROCESSABLE_ENTITY.getCode())
            .build();

        return HttpResponse.unprocessableEntity()
                .body(status);
    }

    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ResourceConflictException exception) {
        var causes = exception.getConflicts().stream()
            .map(conflict -> (StatusCause.builder()
                .message(conflict)
                .build()))
            .collect(Collectors.toList());

        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(String.format("Conflict of %s %s with other Resources", exception.getKind(), exception.getName()))
            .reason("Conflict")
            .details(StatusDetails.builder()
                .kind(exception.getKind())
                .name(exception.getName())
                .causes(causes)
                .build())
            .code(HttpStatus.CONFLICT.getCode())
            .build();

        return HttpResponse.status(HttpStatus.CONFLICT)
                .body(status);
    }


    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ResourceNotFoundException exception) {
        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(String.format("Resource %s %s not found", exception.getKind(), exception.getName()))
            .reason("NotFound")
            .details(StatusDetails.builder()
                .kind(exception.getKind())
                .name(exception.getName())
                .build())
            .code(HttpStatus.NOT_FOUND.getCode())
            .build();

        return HttpResponse.notFound(status);
    }

    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ResourceForbiddenException exception) {
        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(String.format("Resource %s %s forbidden", exception.getKind(), exception.getName()))
            .reason("Forbidden")
            .details(StatusDetails.builder()
                .kind(exception.getKind())
                .name(exception.getName())
                .build())
            .code(HttpStatus.FORBIDDEN.getCode())
            .build();

        return HttpResponse.status(HttpStatus.FORBIDDEN)
                .body(status);
    }

    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ResourceInternalErrorException exception) {
        var causes = exception.getMessage();

        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message("Internal server error")
            .reason("InternalError")
            .details(StatusDetails.builder()
                .causes(List.of(StatusCause.builder()
                        .message(causes)
                        .build()))
                .build())
            .code(HttpStatus.INTERNAL_SERVER_ERROR.getCode())
            .build();

        return HttpResponse.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(status);
    }

}
