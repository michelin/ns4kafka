package com.michelin.ns4kafka.exceptionHandler;

import javax.inject.Singleton;

import com.michelin.ns4kafka.exception.ResourceValidationException;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;

@Produces
@Singleton
@Requires(classes = { ResourceValidationException.class, ExceptionHandler.class })
public class ResourceValidationExceptionHandler
        implements ExceptionHandler<ResourceValidationException, HttpResponse<ResourceCreationError>> {

    @Override
    public HttpResponse<ResourceCreationError> handle(HttpRequest request, ResourceValidationException exception) {
        return HttpResponse.badRequest()
                .body(new ResourceCreationError("Message validation failed", exception.getValidationErrors()));
    }
}
