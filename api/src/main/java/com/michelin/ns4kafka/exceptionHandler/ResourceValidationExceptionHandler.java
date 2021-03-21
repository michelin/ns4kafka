package com.michelin.ns4kafka.exceptionHandler;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import javax.inject.Singleton;

import com.michelin.ns4kafka.validation.ResourceValidationException;

@Produces
@Singleton
@Requires(classes = { ResourceValidationException.class, ExceptionHandler.class })
public class ResourceValidationExceptionHandler
        implements ExceptionHandler<ResourceValidationException, HttpResponse<T>> {

    @Override
    public HttpResponse<ResourceCreationError> handle(HttpRequest<T> request, ResourceValidationException exception) {
        return HttpResponse.badRequest()
                .body(new ResourceCreationError("Message validation failed", exception.getValidationErrors()));
    }
}
