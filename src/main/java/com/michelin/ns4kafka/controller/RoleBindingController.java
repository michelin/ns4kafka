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

import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.service.RoleBindingService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
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

/** Controller to manage role bindings. */
@Tag(name = "Role Bindings", description = "Manage the role bindings.")
@Controller(value = "/api/namespaces/{namespace}/role-bindings")
@ExecuteOn(TaskExecutors.IO)
public class RoleBindingController extends NamespacedResourceController {
    @Inject
    private RoleBindingService roleBindingService;

    /**
     * List role bindings by namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @return A list of role bindings
     */
    @Get
    public List<RoleBinding> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        return roleBindingService.findByWildcardName(namespace, name);
    }

    /**
     * Create a role binding.
     *
     * @param namespace The namespace
     * @param roleBinding The role binding
     * @param dryrun Is dry run mode or not?
     * @return The created role binding
     */
    @Post("{?dryrun}")
    public HttpResponse<RoleBinding> apply(
            String namespace,
            @Valid @Body RoleBinding roleBinding,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        roleBinding.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        roleBinding.getMetadata().setCluster(ns.getMetadata().getCluster());
        roleBinding.getMetadata().setNamespace(namespace);

        Optional<RoleBinding> existingRoleBinding = roleBindingService.findByName(
                namespace, roleBinding.getMetadata().getName());
        if (existingRoleBinding.isPresent() && existingRoleBinding.get().equals(roleBinding)) {
            return formatHttpResponse(existingRoleBinding.get(), ApplyStatus.UNCHANGED);
        }

        ApplyStatus status = existingRoleBinding.isPresent() ? ApplyStatus.CHANGED : ApplyStatus.CREATED;
        if (dryrun) {
            return formatHttpResponse(roleBinding, status);
        }

        sendEventLog(
                roleBinding,
                status,
                existingRoleBinding.<Object>map(RoleBinding::getSpec).orElse(null),
                roleBinding.getSpec(),
                EMPTY_STRING);

        roleBindingService.create(roleBinding);
        return formatHttpResponse(roleBinding, status);
    }

    /**
     * Delete role bindings.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @param dryrun Is dry run mode or not?
     * @return An HTTP response
     */
    @Delete
    @Status(HttpStatus.OK)
    public HttpResponse<List<RoleBinding>> delete(
            String namespace,
            @QueryValue(defaultValue = "*") String name,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        List<RoleBinding> roleBindings = roleBindingService.findByWildcardName(namespace, name);

        if (roleBindings.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.ok(roleBindings);
        }

        roleBindings.forEach(roleBinding -> {
            sendEventLog(roleBinding, ApplyStatus.DELETED, roleBinding.getSpec(), null, EMPTY_STRING);
            roleBindingService.delete(roleBinding);
        });

        return HttpResponse.ok(roleBindings);
    }
}
