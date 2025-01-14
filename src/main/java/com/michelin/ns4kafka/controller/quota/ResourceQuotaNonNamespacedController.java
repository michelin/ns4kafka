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

package com.michelin.ns4kafka.controller.quota;

import com.michelin.ns4kafka.controller.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.model.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.ResourceQuotaService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import java.util.List;

/**
 * Non namespaced resource quota controller.
 */
@Tag(name = "Quotas", description = "Manage the resource quotas.")
@Controller(value = "/api/resource-quotas")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class ResourceQuotaNonNamespacedController extends NonNamespacedResourceController {
    @Inject
    ResourceQuotaService resourceQuotaService;

    @Inject
    NamespaceService namespaceService;

    /**
     * List quotas.
     *
     * @return A list of quotas
     */
    @Get
    public List<ResourceQuotaResponse> listAll() {
        return resourceQuotaService.getUsedQuotaByNamespaces(namespaceService.findAll());
    }
}
