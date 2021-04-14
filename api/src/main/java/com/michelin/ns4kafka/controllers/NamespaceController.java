package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Tag(name = "Namespaces")
@Controller("/api/namespaces")
public class NamespaceController extends NonNamespacedResourceController {

    @Inject
    NamespaceService namespaceService;

    @Post
    public Namespace apply(@Valid @Body Namespace namespace) {

        Optional<Namespace> existingNamespace = namespaceService.findByName(namespace.getMetadata().getName());

        List<String> validationErrors = new ArrayList<>();

        if (existingNamespace.isEmpty()) {
            // New Namespace checks
            validationErrors = namespaceService.validateCreation(namespace);
        } else {
            // Update checks
            //Immutable data
            if (!namespace.getMetadata().getCluster().equals(existingNamespace.get().getMetadata().getCluster())) {
                validationErrors.add("Invalid value " + namespace.getMetadata().getCluster()
                        + " for cluster: Value is immutable ("
                        + existingNamespace.get().getMetadata().getCluster() + ")");
            }
            if (!namespace.getMetadata().getCluster().equals(existingNamespace.get().getMetadata().getCluster())) {
                validationErrors.add("Invalid value " + namespace.getSpec().getKafkaUser()
                        + " for kafkaUser: Value is immutable ("
                        + existingNamespace.get().getSpec().getKafkaUser() + ")");
            }
        }
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }

        return namespaceService.createOrUpdate(namespace);

    }

    @Delete("/{namespace}")
    public HttpResponse delete(String namespace) {
        return null;
    }

}
