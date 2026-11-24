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
package com.michelin.ns4kafka.controller;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.service.ConfluentApiKeyService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyResponse;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.utils.SecurityService;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;

/** Controller to manage Confluent API keys. */
@Tag(name = "Confluent API Keys", description = "Manage the Confluent API keys.")
@Controller(value = "/api/namespaces/{namespace}/api-keys")
@ExecuteOn(TaskExecutors.IO)
public class ConfluentApiKeyController extends NamespacedResourceController {
    private final ConfluentApiKeyService confluentApiKeyService;

    /**
     * Constructor.
     *
     * @param confluentApiKeyService The Confluent API key service
     * @param namespaceService The namespace service
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    protected ConfluentApiKeyController(
            ConfluentApiKeyService confluentApiKeyService,
            NamespaceService namespaceService,
            SecurityService securityService,
            ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        super(namespaceService, securityService, applicationEventPublisher);
        this.confluentApiKeyService = confluentApiKeyService;
    }

    /**
     * List Confluent API keys for the namespace.
     *
     * @param namespace The namespace
     * @return The list of Confluent API keys
     */
    @Get
    public List<ApiKeyResponse> list(String namespace) {
        Namespace ns = getNamespace(namespace);
        return confluentApiKeyService.findAllForNamespace(ns);
    }
}
