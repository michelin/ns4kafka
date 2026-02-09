/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.controller.generic;

import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.MetadataResource;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.security.utils.SecurityService;
import java.time.Instant;
import java.util.Date;

/** Resource controller. */
public abstract class ResourceController {
    private static final String STATUS_HEADER = "X-Ns4kafka-Result";
    protected final SecurityService securityService;
    protected final ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    /**
     * Constructor.
     *
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    protected ResourceController(
            SecurityService securityService, ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        this.securityService = securityService;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    /**
     * Format an HTTP response with the operation status in a header.
     *
     * @param body The response body
     * @param status The operation status
     * @return The formatted HTTP response
     * @param <T> The type of the response body
     */
    public <T> HttpResponse<T> formatHttpResponse(T body, ApplyStatus status) {
        return HttpResponse.ok(body).header(STATUS_HEADER, status.toString());
    }

    /**
     * Send an audit log event.
     *
     * @param resource the resource
     * @param operation the operation
     * @param before the resource before the operation
     * @param after the resource after the operation
     */
    public void sendEventLog(
            MetadataResource resource, ApplyStatus operation, Object before, Object after, String version) {
        AuditLog auditLog = new AuditLog(
                securityService.username().orElse(EMPTY_STRING),
                securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN),
                Date.from(Instant.now()),
                resource.getKind(),
                resource.getMetadata(),
                operation,
                before,
                after,
                version);

        applicationEventPublisher.publishEvent(auditLog);
    }
}
