package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Status;
import com.michelin.ns4kafka.models.Status.StatusDetails;
import com.michelin.ns4kafka.models.Status.StatusPhase;
import com.michelin.ns4kafka.utils.exceptions.ForbiddenNamespaceException;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import com.michelin.ns4kafka.utils.exceptions.UnknownNamespaceException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Error;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthorizationException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.ElementKind;
import jakarta.validation.Path;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Exception handler controller.
 */
@Slf4j
@Controller("/errors")
public class ExceptionHandlerController {
    /**
     * Handle resource validation exception.
     *
     * @param request   the request
     * @param exception the exception
     * @return the http response
     */
    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ResourceValidationException exception) {
        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(String.format("Invalid %s %s", exception.getKind(), exception.getName()))
            .httpStatus(HttpStatus.UNPROCESSABLE_ENTITY)
            .details(StatusDetails.builder()
                .kind(exception.getKind())
                .name(exception.getName())
                .causes(exception.getValidationErrors())
                .build())
            .build();

        return HttpResponse.unprocessableEntity()
            .body(status);
    }

    /**
     * Handle constraint violation exception.
     *
     * @param request   the request
     * @param exception the exception
     * @return the http response
     */
    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ConstraintViolationException exception) {
        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message("Invalid Resource")
            .httpStatus(HttpStatus.UNPROCESSABLE_ENTITY)
            .details(StatusDetails.builder()
                .causes(exception.getConstraintViolations().stream().map(this::formatViolation).toList())
                .build())
            .build();

        return HttpResponse.unprocessableEntity()
            .body(status);
    }

    /**
     * Handle not found exception.
     *
     * @param request the request
     * @return the http response
     */
    @Error(global = true, status = HttpStatus.NOT_FOUND)
    public HttpResponse<Status> error(HttpRequest<?> request) {
        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message("Not Found")
            .httpStatus(HttpStatus.NOT_FOUND)
            .build();

        return HttpResponse.notFound()
            .body(status);
    }

    /**
     * Handle authentication exception.
     *
     * @param request   the request
     * @param exception the exception
     * @return the http response
     */
    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, AuthenticationException exception) {
        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(exception.getMessage())
            .httpStatus(HttpStatus.UNAUTHORIZED)
            .build();

        return HttpResponse.unauthorized()
            .body(status);
    }

    /**
     * Handle authorization exception.
     *
     * @param request   the request
     * @param exception the exception
     * @return the http response
     */
    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, AuthorizationException exception) {
        if (exception.isForbidden()) {
            var status = Status.builder()
                .status(StatusPhase.Failed)
                .message("Resource forbidden")
                .httpStatus(HttpStatus.FORBIDDEN)
                .build();

            return HttpResponse.status(HttpStatus.FORBIDDEN)
                .body(status);
        }

        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(exception.getMessage())
            .httpStatus(HttpStatus.UNAUTHORIZED)
            .build();

        return HttpResponse.unauthorized()
            .body(status);
    }

    /**
     * Handle namespace unknown exception.
     *
     * @param request   the request
     * @param exception the exception
     * @return the http response
     */
    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, UnknownNamespaceException exception) {
        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(exception.getMessage())
            .httpStatus(HttpStatus.UNPROCESSABLE_ENTITY)
            .build();

        return HttpResponse.unprocessableEntity()
            .body(status);
    }

    /**
     * Handle namespace forbidden exception.
     *
     * @param request   the request
     * @param exception the exception
     * @return the http response
     */
    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, ForbiddenNamespaceException exception) {
        var status = Status.builder()
            .status(StatusPhase.Failed)
            .message(exception.getMessage())
            .httpStatus(HttpStatus.FORBIDDEN)
            .build();

        return HttpResponse.status(HttpStatus.FORBIDDEN)
            .body(status);
    }

    /**
     * Handle exception.
     *
     * @param request   the request
     * @param exception the exception
     * @return the http response
     */
    @Error(global = true)
    public HttpResponse<Status> error(HttpRequest<?> request, Exception exception) {
        log.error("An error occurred on API endpoint {} {}: {}", request.getMethodName(),
            request.getUri(), exception.getMessage(), exception);

        Status status = Status.builder()
            .status(StatusPhase.Failed)
            .message("Internal server error")
            .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
            .details(StatusDetails.builder()
                .causes(List.of(exception.getMessage() != null ? exception.getMessage() : exception.toString()))
                .build())
            .build();

        return HttpResponse.serverError()
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
}
