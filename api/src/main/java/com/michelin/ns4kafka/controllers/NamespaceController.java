package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
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

    @Post("{?dryrun}")
    public Namespace apply(@Valid @Body Namespace namespace, @QueryValue(defaultValue = "false") boolean dryrun) {

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

        //dryrun checks
        if (dryrun) {
            return namespace;
        }
        return namespaceService.createOrUpdate(namespace);

    }

    @Delete("/{namespace}{?dryrun}")
    public HttpResponse<Void> delete(String namespace, @QueryValue(defaultValue = "false") boolean dryrun) {

        if (dryrun) {
            return HttpResponse.noContent();
        }

        return HttpResponse.noContent();
    }

}
