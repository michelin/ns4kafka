package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.security.utils.SecurityService;
import lombok.Builder;
import lombok.Data;

import javax.inject.Inject;
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

    public void sendEventLog(String kind, ObjectMeta metadata, String operation, String before, String after) {
        var auditLog = new AuditLog(securityService, Date.from(Instant.now()), kind, metadata, operation, before, after);
        applicationEventPublisher.publishEvent(auditLog);
    }

    public String returnStringOfSpec(Object spec) {
        if (spec == null) {
            return null;
        }
        return spec.toString();
    }

    @Builder
    @Data
    public static class AuditLog {

        private SecurityService user;
        private Date date;
        private String kind;
        private ObjectMeta metadata;
        private String operation;
        private String before;
        private String after;


    }

}
