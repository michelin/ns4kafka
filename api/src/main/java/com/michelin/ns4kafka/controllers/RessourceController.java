package com.michelin.ns4kafka.controllers;

import io.micronaut.http.HttpResponse;

public abstract class RessourceController {

    public final String statusHeaderName = "X-Ns4kafka-Result";

    public <T> HttpResponse<T> formatHttpResponse(T body, ApplyStatus status) {
        return HttpResponse.ok(body).header(statusHeaderName, status.toString());
    }
}
