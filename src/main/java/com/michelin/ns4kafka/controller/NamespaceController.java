package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidImmutableValue;
import static com.michelin.ns4kafka.util.enumation.Kind.NAMESPACE;

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
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Controller to manage the namespaces.
 */
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Tag(name = "Namespaces", description = "Manage the namespaces.")
@Controller("/api/namespaces")
public class NamespaceController extends NonNamespacedResourceController {
    @Inject
    NamespaceService namespaceService;

    /**
     * List namespaces.
     *
     * @return A list of namespaces
     */
    @Get("/")
    public List<Namespace> list() {
        return namespaceService.listAll();
    }

    /**
     * Get a namespace by name.
     *
     * @param namespace The namespace
     * @return A namespace
     */
    @Get("/{namespace}")
    public Optional<Namespace> get(String namespace) {
        return namespaceService.findByName(namespace);
    }

    /**
     * Create a namespace.
     *
     * @param namespace The namespace
     * @param dryrun    Does the creation is a dry run
     * @return The created namespace
     */
    @Post("{?dryrun}")
    public HttpResponse<Namespace> apply(@Valid @Body Namespace namespace,
                                         @QueryValue(defaultValue = "false") boolean dryrun) {
        Optional<Namespace> existingNamespace = namespaceService.findByName(namespace.getMetadata().getName());

        List<String> validationErrors = new ArrayList<>();
        if (existingNamespace.isEmpty()) {
            validationErrors.addAll(namespaceService.validateCreation(namespace));
        } else {
            if (!namespace.getMetadata().getCluster().equals(existingNamespace.get().getMetadata().getCluster())) {
                validationErrors.add(invalidImmutableValue("cluster",
                    existingNamespace.get().getMetadata().getCluster()));
            }
        }

        validationErrors.addAll(namespaceService.validate(namespace));

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(namespace, validationErrors);
        }

        namespace.getMetadata().setNamespace(namespace.getMetadata().getName());
        namespace.getMetadata().setCreationTimestamp(Date.from(Instant.now()));

        if (existingNamespace.isPresent() && existingNamespace.get().equals(namespace)) {
            return formatHttpResponse(existingNamespace.get(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingNamespace.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        if (dryrun) {
            return formatHttpResponse(namespace, status);
        }

        sendEventLog(namespace, status, existingNamespace.<Object>map(Namespace::getSpec).orElse(null),
            namespace.getSpec());

        return formatHttpResponse(namespaceService.createOrUpdate(namespace), status);
    }

    /**
     * Delete a namespace.
     *
     * @param namespace The namespace
     * @param dryrun    Is dry run mode or not ?
     * @return An HTTP response
     */
    @Delete("/{namespace}{?dryrun}")
    public HttpResponse<Void> delete(String namespace, @QueryValue(defaultValue = "false") boolean dryrun) {
        Optional<Namespace> optionalNamespace = namespaceService.findByName(namespace);
        if (optionalNamespace.isEmpty()) {
            return HttpResponse.notFound();
        }

        List<String> namespaceResources = namespaceService.listAllNamespaceResources(optionalNamespace.get());
        if (!namespaceResources.isEmpty()) {
            List<String> validationErrors = namespaceResources
                .stream()
                .map(FormatErrorUtils::invalidNamespaceDeleteOperation)
                .toList();
            throw new ResourceValidationException(NAMESPACE, namespace, validationErrors);
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        var namespaceToDelete = optionalNamespace.get();
        sendEventLog(namespaceToDelete, ApplyStatus.deleted, namespaceToDelete.getSpec(), null);
        namespaceService.delete(optionalNamespace.get());
        return HttpResponse.noContent();
    }
}
