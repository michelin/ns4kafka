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
package com.michelin.ns4kafka.controller.connect;

import com.michelin.ns4kafka.controller.generic.ResourceController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Resource.Metadata.Phase;
import com.michelin.ns4kafka.model.connect.Connector;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.ConnectorService;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.utils.SecurityService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import java.util.Collection;
import org.jspecify.annotations.Nullable;

/** Non-namespaced controller to manage connectors. */
@Tag(name = "Connectors", description = "Manage the connectors.")
@Controller(value = "/api/connectors")
@ExecuteOn(TaskExecutors.IO)
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class ConnectorNonNamespacedController extends ResourceController {
    private final ConnectorService connectorService;

    /**
     * Constructor.
     *
     * @param connectorService The connector service
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    protected ConnectorNonNamespacedController(
            ConnectorService connectorService,
            SecurityService securityService,
            ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        super(securityService, applicationEventPublisher);
        this.connectorService = connectorService;
    }

    /**
     * List all connectors, filtered by the given metadata status phase.
     *
     * @param phase The phase filter. If not provided, all connectors are returned
     * @return A list of connectors
     */
    @Get
    public Collection<Connector> listAll(@QueryValue @Nullable Phase phase) {
        return connectorService.findAllByPhase(phase);
    }
}
