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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidImmutableValue;
import static com.michelin.ns4kafka.util.enumation.Kind.NAMESPACE;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.util.FormatErrorUtils;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Controller to manage the namespaces. */
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Tag(name = "Namespaces", description = "Manage the namespaces.")
@Controller("/api/namespaces")
public class NamespaceController extends NonNamespacedResourceController {
    @Inject
    private NamespaceService namespaceService;

    /**
     * List namespaces, which can be filtered based on optional search query parameters.
     *
     * @param search The map of optional query parameters used to filter namespaces. Supported parameters are: - name:
     *     matches the namespace name. Wildcard supported. - topic: find namespace owner of the given topic name.
     * @return A list of namespaces
     */
    @Get("{?search*}")
    public List<Namespace> list(@QueryValue Map<String, String> search) {
        List<Namespace> namespaces = namespaceService.findByWildcardName(search.getOrDefault("name", "*"));

        return search.containsKey("topic")
                ? namespaceService
                        .findByTopicName(namespaces, search.get("topic"))
                        .map(Collections::singletonList)
                        .orElse(List.of())
                : namespaces;
    }

    /**
     * Create a namespace.
     *
     * @param namespace The namespace
     * @param dryrun Is dry run mode or not?
     * @return The created namespace
     */
    @Post("{?dryrun}")
    public HttpResponse<Namespace> apply(
            @Valid @Body Namespace namespace, @QueryValue(defaultValue = "false") boolean dryrun) {
        Optional<Namespace> existingNamespace =
                namespaceService.findByName(namespace.getMetadata().getName());

        List<String> validationErrors = new ArrayList<>();
        if (existingNamespace.isEmpty()) {
            validationErrors.addAll(namespaceService.validateCreation(namespace));
        } else {
            if (!namespace
                    .getMetadata()
                    .getCluster()
                    .equals(existingNamespace.get().getMetadata().getCluster())) {
                validationErrors.add(invalidImmutableValue(
                        "cluster", existingNamespace.get().getMetadata().getCluster()));
            }
        }

        validationErrors.addAll(namespaceService.validate(namespace));

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(namespace, validationErrors);
        }

        namespace.getMetadata().setNamespace(namespace.getMetadata().getName());
        namespace.getMetadata().setCreationTimestamp(Date.from(Instant.now()));

        if (existingNamespace.isPresent() && existingNamespace.get().equals(namespace)) {
            return formatHttpResponse(existingNamespace.get(), ApplyStatus.UNCHANGED);
        }

        ApplyStatus status = existingNamespace.isPresent() ? ApplyStatus.CHANGED : ApplyStatus.CREATED;

        if (dryrun) {
            return formatHttpResponse(namespace, status);
        }

        sendEventLog(
                namespace,
                status,
                existingNamespace.<Object>map(Namespace::getSpec).orElse(null),
                namespace.getSpec(),
                EMPTY_STRING);

        return formatHttpResponse(namespaceService.createOrUpdate(namespace), status);
    }

    /**
     * Delete namespaces.
     *
     * @param dryrun Is dry run mode or not?
     * @param name The name parameter
     * @return An HTTP response
     */
    @Delete
    public HttpResponse<List<Namespace>> delete(
            @QueryValue(defaultValue = "*") String name, @QueryValue(defaultValue = "false") boolean dryrun) {
        List<Namespace> namespaces = namespaceService.findByWildcardName(name);

        if (namespaces.isEmpty()) {
            return HttpResponse.notFound();
        }

        List<String> namespaceResources = namespaces.stream()
                .flatMap(namespace -> namespaceService.findAllResourcesByNamespace(namespace).stream())
                .toList();

        if (!namespaceResources.isEmpty()) {
            List<String> validationErrors = namespaceResources.stream()
                    .map(FormatErrorUtils::invalidNamespaceDeleteOperation)
                    .toList();

            throw new ResourceValidationException(
                    NAMESPACE,
                    String.join(
                            ",",
                            namespaces.stream()
                                    .map(namespace -> namespace.getMetadata().getName())
                                    .toList()),
                    validationErrors);
        }

        if (dryrun) {
            return HttpResponse.ok(namespaces);
        }

        namespaces.forEach(namespace -> {
            sendEventLog(namespace, ApplyStatus.DELETED, namespace.getSpec(), null, EMPTY_STRING);

            namespaceService.delete(namespace);
        });

        return HttpResponse.ok(namespaces);
    }
}
