package com.michelin.ns4kafka.controllers;


import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.services.RoleBindingService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Tag(name = "Role Bindings")
@Controller(value = "/api/namespaces/{namespace}/role-bindings")
@ExecuteOn(TaskExecutors.IO)
public class RoleBindingController extends NamespacedResourceController {
    /**
     * The role binding service
     */
    @Inject
    RoleBindingService roleBindingService;

    /**
     * Get all the role bindings by namespace
     * @param namespace The namespace
     * @return A list of role bindings
     */
    @Get
    public List<RoleBinding> list(String namespace) {
        return roleBindingService.list(namespace);
    }

    /**
     * Get a role binding by namespace and subject
     * @param namespace The namespace
     * @param name The role binding name
     * @return A role binding
     */
    @Get("/{name}")
    public Optional<RoleBinding> get(String namespace, String name) {
        return roleBindingService.findByName(namespace, name);
    }

    /**
     * Publish a role binding
     * @param namespace The namespace
     * @param roleBinding The role binding
     * @param dryrun Does the creation is a dry run
     * @return The created role binding
     */
    @Post("{?dryrun}")
    public HttpResponse<RoleBinding> apply(String namespace, @Valid @Body RoleBinding roleBinding, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        roleBinding.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        roleBinding.getMetadata().setCluster(ns.getMetadata().getCluster());
        roleBinding.getMetadata().setNamespace(namespace);

        Optional<RoleBinding> existingRoleBinding = roleBindingService.findByName(namespace, roleBinding.getMetadata().getName());
        if (existingRoleBinding.isPresent() && existingRoleBinding.get().equals(roleBinding)) {
            return formatHttpResponse(existingRoleBinding.get(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingRoleBinding.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
        if (dryrun) {
            return formatHttpResponse(roleBinding, status);
        }

        sendEventLog(roleBinding.getKind(),
                roleBinding.getMetadata(),
                status,
                existingRoleBinding.<Object>map(RoleBinding::getSpec).orElse(null),
                roleBinding.getSpec());
        roleBindingService.create(roleBinding);
        return formatHttpResponse(roleBinding, status);
    }

    /**
     * Delete a role binding
     * @param namespace The namespace
     * @param name The role binding
     * @param dryrun Is dry run mode or not ?
     * @return An HTTP response
     */
    @Delete("/{name}{?dryrun}")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(String namespace, String name, @QueryValue(defaultValue = "false") boolean dryrun) {
        Optional<RoleBinding> roleBinding = roleBindingService.findByName(namespace, name);

        if (roleBinding.isEmpty()) {
            throw new ResourceValidationException(
                    List.of("Invalid value " + name + " for name : Role Binding doesn't exist in this namespace"),
                    "RoleBinding",
                    name
            );
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        var roleBindingToDelete = roleBinding.get();
        sendEventLog(roleBindingToDelete .getKind(),
                roleBindingToDelete.getMetadata(),
                ApplyStatus.deleted,
                roleBindingToDelete.getSpec(),
                null);
        roleBindingService.delete(roleBindingToDelete);
        return HttpResponse.noContent();
    }
}
