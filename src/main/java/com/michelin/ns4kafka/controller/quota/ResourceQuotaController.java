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

import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.model.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.service.ResourceQuotaService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Resource quota controller.
 */
@Tag(name = "Quotas", description = "Manage the resource quotas.")
@Controller(value = "/api/namespaces/{namespace}/resource-quotas")
@ExecuteOn(TaskExecutors.IO)
public class ResourceQuotaController extends NamespacedResourceController {
    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * List quotas by namespace.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @return A list of quotas
     */
    @Get
    public List<ResourceQuotaResponse> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        return resourceQuotaService.findByWildcardName(namespace, name)
            .stream()
            .map(resourceQuota -> resourceQuotaService.getUsedResourcesByQuotaByNamespace(getNamespace(namespace),
                Optional.of(resourceQuota)))
            .toList();
    }

    /**
     * Get a quota by namespace and name.
     *
     * @param namespace The name
     * @param quota     The quota name
     * @return A quota
     * @deprecated use {@link #list(String, String)} instead.
     */
    @Get("/{quota}")
    @Deprecated(since = "1.12.0")
    public Optional<ResourceQuotaResponse> get(String namespace, String quota) {
        Optional<ResourceQuota> resourceQuota = resourceQuotaService.findByName(namespace, quota);
        if (resourceQuota.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
            resourceQuotaService.getUsedResourcesByQuotaByNamespace(getNamespace(namespace), resourceQuota));
    }

    /**
     * Create a quota.
     *
     * @param namespace The namespace
     * @param quota     The resource quota
     * @param dryrun    Is dry run mode or not?
     * @return The created quota
     */
    @Post("{?dryrun}")
    public HttpResponse<ResourceQuota> apply(String namespace,
                                             @Body @Valid ResourceQuota quota,
                                             @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        quota.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        quota.getMetadata().setCluster(ns.getMetadata().getCluster());
        quota.getMetadata().setNamespace(namespace);

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, quota);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(quota, validationErrors);
        }

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findForNamespace(namespace);
        if (resourceQuotaOptional.isPresent() && resourceQuotaOptional.get().equals(quota)) {
            return formatHttpResponse(quota, ApplyStatus.UNCHANGED);
        }

        ApplyStatus status = resourceQuotaOptional.isPresent() ? ApplyStatus.CHANGED : ApplyStatus.CREATED;
        if (dryrun) {
            return formatHttpResponse(quota, status);
        }

        sendEventLog(
            quota,
            status,
            resourceQuotaOptional.<Object>map(ResourceQuota::getSpec).orElse(null),
            quota.getSpec(),
            EMPTY_STRING
        );

        return formatHttpResponse(resourceQuotaService.create(quota), status);
    }

    /**
     * Delete quotas.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @param dryrun    Is dry run mode or not?
     * @return An HTTP response
     */
    @Delete
    @Status(HttpStatus.OK)
    public HttpResponse<List<ResourceQuota>> bulkDelete(String namespace,
                                                        @QueryValue(defaultValue = "*") String name,
                                                        @QueryValue(defaultValue = "false") boolean dryrun) {

        List<ResourceQuota> resourceQuotas = resourceQuotaService.findByWildcardName(namespace, name);

        if (resourceQuotas.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.ok(resourceQuotas);
        }

        resourceQuotas.forEach(resourceQuota -> {
            sendEventLog(
                resourceQuota,
                ApplyStatus.DELETED,
                resourceQuota.getSpec(),
                null,
                EMPTY_STRING
            );
            resourceQuotaService.delete(resourceQuota);
        });

        return HttpResponse.ok(resourceQuotas);
    }

    /**
     * Delete a quota.
     *
     * @param namespace The namespace
     * @param name      The resource quota
     * @param dryrun    Is dry run mode or not?
     * @return An HTTP response
     * @deprecated use {@link #bulkDelete(String, String, boolean)} instead.
     */
    @Delete("/{name}{?dryrun}")
    @Deprecated(since = "1.13.0")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(String namespace,
                                     String name,
                                     @QueryValue(defaultValue = "false") boolean dryrun) {
        Optional<ResourceQuota> resourceQuota = resourceQuotaService.findByName(namespace, name);
        if (resourceQuota.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        ResourceQuota resourceQuotaToDelete = resourceQuota.get();

        sendEventLog(
            resourceQuotaToDelete,
            ApplyStatus.DELETED,
            resourceQuotaToDelete.getSpec(),
            null,
            EMPTY_STRING
        );

        resourceQuotaService.delete(resourceQuotaToDelete);
        return HttpResponse.noContent();
    }
}
