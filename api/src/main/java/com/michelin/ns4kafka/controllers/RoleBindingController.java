package com.michelin.ns4kafka.controllers;


import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.services.RoleBindingService;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.List;
import java.util.Optional;

@Tag(name = "Role Bindings")
@Controller(value = "/api/namespaces/{namespace}/role-bindings")
@ExecuteOn(TaskExecutors.IO)
public class RoleBindingController extends NamespacedResourceController {


    @Inject
    RoleBindingService roleBindingService;

    @Get
    public List<RoleBinding> list(String namespace) {

        // ToDo Custom error message for non existing namespace
        return roleBindingService.list(namespace);
    }

    @Get("/{name}")
    public Optional<RoleBinding> get(String namespace, String name) {


        // ToDo Custom error for non existing namespace
        return roleBindingService.findByName(namespace, name);
    }

    @Post("{?dryrun}")
    public RoleBinding apply(String namespace, @Valid @Body RoleBinding rolebinding, @QueryValue(defaultValue = "false") boolean dryrun) {

        // fill with cluster name
        Namespace ns = getNamespace(namespace);
        rolebinding.getMetadata().setCluster(ns.getMetadata().getCluster());
        rolebinding.getMetadata().setNamespace(namespace);

        if (dryrun) {
            return rolebinding;
        }
        roleBindingService.create(rolebinding);
        return rolebinding;
    }

    @Delete("/{name}")
    @Status(HttpStatus.NO_CONTENT)
    public void delete(String namespace, String name) {

        // ToDo duplicated with Access Control
        Optional<RoleBinding> roleBinding = roleBindingService.findByName(namespace, name);

        if (roleBinding.isEmpty()) {
            throw new ResourceValidationException(List.of("Invalid value " + name + " for name : Role Binding doesn't exist in this namespace"));
        }
        roleBindingService.delete(roleBinding.get());
    }
}
