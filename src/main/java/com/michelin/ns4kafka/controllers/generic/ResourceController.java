package com.michelin.ns4kafka.controllers.generic;

import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.MetadataResource;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Date;

/**
 * Resource controller.
 */
public abstract class ResourceController {
    private static final String STATUS_HEADER = "X-Ns4kafka-Result";

    @Inject
    public SecurityService securityService;

    @Inject
    public ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    public <T> HttpResponse<T> formatHttpResponse(T body, ApplyStatus status) {
        return HttpResponse.ok(body).header(STATUS_HEADER, status.toString());
    }

    /**
     * Send an audit log event.
     *
     * @param resource  the resource
     * @param operation the operation
     * @param before    the resource before the operation
     * @param after     the resource after the operation
     */
    public void sendEventLog(MetadataResource resource, ApplyStatus operation, Object before,
                             Object after) {
        AuditLog auditLog = new AuditLog(securityService.username().orElse(""),
            securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN), Date.from(Instant.now()),
            resource.getKind(), resource.getMetadata(), operation, before, after);
        applicationEventPublisher.publishEvent(auditLog);
    }
}
