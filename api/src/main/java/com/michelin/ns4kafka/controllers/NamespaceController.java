package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Tag(name = "Namespaces")
@Controller("/api/namespaces")
public class NamespaceController extends NonNamespacedResourceController {

    @Inject
    NamespaceService namespaceService;

    @Get("/")
    public List<Namespace> list() {
        log.info("list Namespace received");
        return namespaceService.listAll();
    }

    @Get("/{namespace}")
    public Optional<Namespace> get(String namespace) {
        log.info("Get Namespace {} received", namespace);
        return namespaceService.findByName(namespace);
    }

    @Post("{?dryrun}")
    public Namespace apply(@Valid @Body Namespace namespace, @QueryValue(defaultValue = "false") boolean dryrun) {
        log.info("Apply Namespace {} received", namespace.getMetadata().getName());

        Optional<Namespace> existingNamespace = namespaceService.findByName(namespace.getMetadata().getName());

        List<String> validationErrors = new ArrayList<>();

        if (existingNamespace.isEmpty()) {
            // New Namespace checks
            validationErrors.addAll(namespaceService.validateCreation(namespace));
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
        // connect cluster check
        validationErrors.addAll(namespaceService.validate(namespace));

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        //augment
        namespace.getMetadata().setCreationTimestamp(Date.from(Instant.now()));

        if (existingNamespace.isPresent() && existingNamespace.get().equals(namespace)) {
            return existingNamespace.get();
        }
        //dryrun checks
        if (dryrun) {
            return namespace;
        }
        return namespaceService.createOrUpdate(namespace);

    }

    @Delete("/{namespace}{?dryrun}")
    public HttpResponse<?> delete(String namespace, @QueryValue(defaultValue = "false") boolean dryrun) {
        log.info("Delete Namespace {} received", namespace);
        // exists ?
        Optional<Namespace> optionalNamespace = namespaceService.findByName(namespace);
        if (optionalNamespace.isEmpty())
            return HttpResponse.notFound();
        // check existing resources
        List<String> namespaceResources = namespaceService.listAllNamespaceResources(optionalNamespace.get());
        if (!namespaceResources.isEmpty()) {
            throw new ResourceValidationException(namespaceResources.stream()
                    .map(s -> "Namespace resource must be deleted first :" + s)
                    .collect(Collectors.toList()));
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }
        namespaceService.delete(optionalNamespace.get());
        return HttpResponse.noContent();
    }

}
