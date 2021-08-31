package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.security.utils.SecurityService;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public abstract class ResourceController {

    @Inject
    SecurityService securityService;

    @Inject
    ApplicationEventPublisher applicationEventPublisher;

    public final String statusHeaderName = "X-Ns4kafka-Result";

    public <T> HttpResponse<T> formatHttpResponse(T body, ApplyStatus status) {
        return HttpResponse.ok(body).header(statusHeaderName, status.toString());
    }

    public void sendEventLog(String kind, ObjectMeta metadata, ApplyStatus operation, Object before, Object after) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss:mm:HH dd/MM/yyyy");
        var instant = Instant.now();
        var auditLog = new AuditLog(securityService.username(), securityService.hasRole("Admin"), sdf.format(Date.from(instant)), kind, metadata, operation, before, after);
        applicationEventPublisher.publishEvent(auditLog);
    }
}
