package com.michelin.ns4kafka.services.executors;

import io.micronaut.core.convert.value.MutableConvertibleValues;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;

import java.util.Optional;

public class HttpResponseMock implements HttpResponse<Void> {
    @Override
    public HttpStatus getStatus() {
        return null;
    }

    @Override
    public HttpHeaders getHeaders() {
        return null;
    }

    @Override
    public MutableConvertibleValues<Object> getAttributes() {
        return null;
    }

    @Override
    public Optional<Void> getBody() {
        return Optional.empty();
    }
}
