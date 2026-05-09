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

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.service.NamespaceService;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.security.utils.SecurityService;
import java.time.Instant;
import java.util.Date;

/** Namespaced resource controller. */
public abstract class NamespacedResourceController extends ResourceController {
    private final NamespaceService namespaceService;

    /**
     * Constructor.
     *
     * @param namespaceService The namespace service
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    protected NamespacedResourceController(
            NamespaceService namespaceService,
            SecurityService securityService,
            ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        super(securityService, applicationEventPublisher);
        this.namespaceService = namespaceService;
    }

    /**
     * Call this to get the Namespace associated with the current request.
     *
     * @param namespace the namespace String
     * @return the Namespace associated with the current request.
     */
    public Namespace getNamespace(String namespace) {
        return namespaceService.findByName(namespace).orElseThrow();
    }

    /**
     * Assign server-side metadata fields to a resource.
     *
     * @param resource The resource
     * @param ns The namespace
     * @param existingResource The existing resource, or null if none
     */
    protected void assignResourceMetadata(Resource resource, Namespace ns, @Nullable Resource existingResource) {
        resource.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        resource.getMetadata().setUpdateTimestamp(resource.getMetadata().getCreationTimestamp());
        resource.getMetadata()
                .setGeneration(
                        existingResource != null
                                ? existingResource.getMetadata().getGeneration()
                                : 0);
        resource.getMetadata().setCluster(ns.getMetadata().getCluster());
        resource.getMetadata().setNamespace(ns.getMetadata().getName());
    }
}
