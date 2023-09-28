package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Status;
import com.michelin.ns4kafka.models.Status.StatusDetails;
import com.michelin.ns4kafka.models.Status.StatusPhase;
import com.michelin.ns4kafka.models.Status.StatusReason;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthorizationException;
import lombok.extern.slf4j.Slf4j;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.ElementKind;
import jakarta.validation.Path;
import java.util.Iterator;
import java.util.List;

@Slf4j
@Controller("/errors")
public class ExceptionHandlerController {
    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ResourceValidationException exception) {
        var status = Status.builder()
                .status(StatusPhase.Failed)
                .message(String.format("Invalid %s %s", exception.getKind(), exception.getName()))
                .reason(StatusReason.Invalid)
                .details(StatusDetails.builder()
                        .kind(exception.getKind())
                        .name(exception.getName())
                        .causes(exception.getValidationErrors())
                        .build())
                .code(HttpStatus.UNPROCESSABLE_ENTITY.getCode())
                .build();

        return HttpResponse.unprocessableEntity()
                .body(status);
    }

    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ConstraintViolationException exception) {
        var status = Status.builder()
                .status(StatusPhase.Failed)
                .message("Invalid Resource")
                .reason(StatusReason.Invalid)
                .details(StatusDetails.builder()
                        .causes(exception.getConstraintViolations().stream().map(this::formatViolation).toList())
                        .build())
                .code(HttpStatus.UNPROCESSABLE_ENTITY.getCode())
                .build();

        return HttpResponse.unprocessableEntity()
                .body(status);
    }

    private String formatViolation(ConstraintViolation<?> violation) {
        Path propertyPath = violation.getPropertyPath();
        StringBuilder message = new StringBuilder();
        Iterator<Path.Node> i = propertyPath.iterator();
        while (i.hasNext()) {
            Path.Node node = i.next();
            if (node.getKind() == ElementKind.METHOD || node.getKind() == ElementKind.CONSTRUCTOR) {
                continue;
            }
            message.append(node.getName());
            if (i.hasNext()) {
                message.append('.');
            }
        }
        message.append(": ").append(violation.getMessage());
        return message.toString();
    }

    @Error(global = true, status = HttpStatus.NOT_FOUND)
    public HttpResponse<Status> error(HttpRequest<?> request) {
        var status = Status.builder()
                .status(StatusPhase.Failed)
                .message("Not Found")
                .reason(StatusReason.NotFound)
                .code(HttpStatus.NOT_FOUND.getCode())
                .build();

        return HttpResponse.status(HttpStatus.NOT_FOUND)
                .body(status);
    }

    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, AuthenticationException exception) {
        var status = Status.builder()
                .status(StatusPhase.Failed)
                .message(exception.getMessage())
                .reason(StatusReason.Unauthorized)
                .code(HttpStatus.UNAUTHORIZED.getCode())
                .build();
        return HttpResponse.unauthorized().body(status);
    }

    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, AuthorizationException exception) {
        if (exception.isForbidden()) {
            var status = Status.builder()
                    .status(StatusPhase.Failed)
                    .message("Resource forbidden")
                    .reason(StatusReason.Forbidden)
                    .code(HttpStatus.FORBIDDEN.getCode())
                    .build();
            return HttpResponse.status(HttpStatus.FORBIDDEN)
                    .body(status);
        }

        var status = Status.builder()
                .status(StatusPhase.Failed)
                .message(exception.getMessage())
                .reason(StatusReason.Unauthorized)
                .code(HttpStatus.UNAUTHORIZED.getCode())
                .build();

        return HttpResponse.unauthorized().body(status);

    }

    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, Exception exception) {
        log.error("An error occurred on API endpoint {} {}: {}", request.getMethodName(),
                request.getUri(), exception.getMessage(), exception);

        Status status = Status.builder()
                .status(StatusPhase.Failed)
                .message("Internal server error")
                .reason(StatusReason.InternalError)
                .details(StatusDetails.builder()
                        .causes(List.of(exception.getMessage() != null ? exception.getMessage() : exception.toString()))
                        .build())
                .code(HttpStatus.INTERNAL_SERVER_ERROR.getCode())
                .build();

        return HttpResponse
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(status);
    }
}
