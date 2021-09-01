package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.security.utils.SecurityService;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Date;

public abstract class ResourceController {

    @Inject
    public SecurityService securityService;

    @Inject
    public ApplicationEventPublisher applicationEventPublisher;

    public final String statusHeaderName = "X-Ns4kafka-Result";

    public <T> HttpResponse<T> formatHttpResponse(T body, ApplyStatus status) {
        return HttpResponse.ok(body).header(statusHeaderName, status.toString());
    }

    public void sendEventLog(String kind, ObjectMeta metadata, ApplyStatus operation, Object before, Object after) {
        var auditLog = new AuditLog(
                securityService.username().orElse(""),
                securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN),
                Date.from(Instant.now()),
                kind, metadata, operation, before, after);
        applicationEventPublisher.publishEvent(auditLog);
    }
}
