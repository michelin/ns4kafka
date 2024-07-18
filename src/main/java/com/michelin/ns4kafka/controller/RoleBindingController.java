package com.michelin.ns4kafka.controller;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.query.RoleBindingFilterParams;
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

/**
 * Controller to manage role bindings.
 */
@Tag(name = "Role Bindings", description = "Manage the role bindings.")
@Controller(value = "/api/namespaces/{namespace}/role-bindings")
@ExecuteOn(TaskExecutors.IO)
public class RoleBindingController extends NamespacedResourceController {
    @Inject
    RoleBindingService roleBindingService;

    /**
     * List role bindings by namespace.
     *
     * @param namespace The namespace
     * @return A list of role bindings
     */
    @Get
    public List<RoleBinding> list(String namespace, @QueryValue Optional<List<String>> name) {
        RoleBindingFilterParams params = RoleBindingFilterParams.builder()
            .name(name.orElse(List.of("*")))
            .build();
        return roleBindingService.list(namespace, params);
    }

    /**
     * Get a role binding by namespace and name.
     *
     * @param namespace The namespace
     * @param name      The role binding name
     * @return A role binding
     */
    @Get("/{name}")
    public Optional<RoleBinding> get(String namespace, String name) {
        return roleBindingService.findByName(namespace, name);
    }

    /**
     * Create a role binding.
     *
     * @param namespace   The namespace
     * @param roleBinding The role binding
     * @param dryrun      Does the creation is a dry run
     * @return The created role binding
     */
    @Post("{?dryrun}")
    public HttpResponse<RoleBinding> apply(String namespace, @Valid @Body RoleBinding roleBinding,
                                           @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        roleBinding.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        roleBinding.getMetadata().setCluster(ns.getMetadata().getCluster());
        roleBinding.getMetadata().setNamespace(namespace);

        Optional<RoleBinding> existingRoleBinding =
            roleBindingService.findByName(namespace, roleBinding.getMetadata().getName());
        if (existingRoleBinding.isPresent() && existingRoleBinding.get().equals(roleBinding)) {
            return formatHttpResponse(existingRoleBinding.get(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingRoleBinding.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
        if (dryrun) {
            return formatHttpResponse(roleBinding, status);
        }

        sendEventLog(roleBinding, status, existingRoleBinding.<Object>map(RoleBinding::getSpec).orElse(null),
            roleBinding.getSpec());
        roleBindingService.create(roleBinding);
        return formatHttpResponse(roleBinding, status);
    }

    /**
     * Delete a role binding.
     *
     * @param namespace The namespace
     * @param name      The role binding
     * @param dryrun    Is dry run mode or not ?
     * @return An HTTP response
     */
    @Delete("/{name}{?dryrun}")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(String namespace, String name,
                                     @QueryValue(defaultValue = "false") boolean dryrun) {
        Optional<RoleBinding> roleBinding = roleBindingService.findByName(namespace, name);
        if (roleBinding.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        var roleBindingToDelete = roleBinding.get();
        sendEventLog(roleBindingToDelete, ApplyStatus.deleted, roleBindingToDelete.getSpec(), null);
        roleBindingService.delete(roleBindingToDelete);
        return HttpResponse.noContent();
    }
}
