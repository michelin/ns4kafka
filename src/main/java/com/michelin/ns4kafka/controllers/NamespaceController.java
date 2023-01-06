package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;

import javax.annotation.security.RolesAllowed;
import javax.validation.Valid;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Tag(name = "Namespaces")
@Controller("/api/namespaces")
public class NamespaceController extends NonNamespacedResourceController {
    @Inject
    NamespaceService namespaceService;

    @Get("/")
    public List<Namespace> list() {
        return namespaceService.listAll();
    }

    @Get("/{namespace}")
    public Optional<Namespace> get(String namespace) {
        return namespaceService.findByName(namespace);
    }

    @Post("{?dryrun}")
    public HttpResponse<Namespace> apply(@Valid @Body Namespace namespace, @QueryValue(defaultValue = "false") boolean dryrun) {

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
            throw new ResourceValidationException(validationErrors, namespace.getKind(), namespace.getMetadata().getName());
        }
        //augment
        namespace.getMetadata().setCreationTimestamp(Date.from(Instant.now()));

        if (existingNamespace.isPresent() && existingNamespace.get().equals(namespace)) {
            return formatHttpResponse(existingNamespace.get(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingNamespace.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        //dryrun checks
        if (dryrun) {
            return formatHttpResponse(namespace, status);
        }

        sendEventLog(namespace.getKind(),
                namespace.getMetadata(),
                status,
                existingNamespace.isPresent() ? existingNamespace.get().getSpec() : null,
                namespace.getSpec());
        return formatHttpResponse(namespaceService.createOrUpdate(namespace), status);

    }

    @Delete("/{namespace}{?dryrun}")
    public HttpResponse<?> delete(String namespace, @QueryValue(defaultValue = "false") boolean dryrun) {
        // exists ?
        Optional<Namespace> optionalNamespace = namespaceService.findByName(namespace);
        if (optionalNamespace.isEmpty())
            return HttpResponse.notFound();
        // check existing resources
        List<String> namespaceResources = namespaceService.listAllNamespaceResources(optionalNamespace.get());
        if (!namespaceResources.isEmpty()) {
            var validationErrors = namespaceResources.stream()
                    .map(s -> "Namespace resource must be deleted first :" + s)
                    .collect(Collectors.toList());
            throw new ResourceValidationException(validationErrors, "Namespace", namespace);
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        var namespaceToDelete = optionalNamespace.get();
        sendEventLog(namespaceToDelete.getKind(),
                namespaceToDelete.getMetadata(),
                ApplyStatus.deleted,
                namespaceToDelete.getSpec(),
                null);
        namespaceService.delete(optionalNamespace.get());
        return HttpResponse.noContent();
    }

}
