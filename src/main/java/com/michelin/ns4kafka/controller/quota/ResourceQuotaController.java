package com.michelin.ns4kafka.controller.quota;

import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.model.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.service.ResourceQuotaService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
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
 * Resource quota controller.
 */
@Tag(name = "Quotas", description = "Manage the resource quotas.")
@Controller(value = "/api/namespaces/{namespace}/resource-quotas")
@ExecuteOn(TaskExecutors.IO)
public class ResourceQuotaController extends NamespacedResourceController {
    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * List quotas by namespace.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @return A list of quotas
     */
    @Get
    public List<ResourceQuotaResponse> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        return resourceQuotaService.findByWildcardName(namespace, name)
            .stream()
            .map(resourceQuota -> resourceQuotaService.getUsedResourcesByQuotaByNamespace(getNamespace(namespace),
                Optional.of(resourceQuota)))
            .toList();
    }

    /**
     * Get a quota by namespace and name.
     *
     * @param namespace The name
     * @param quota     The quota name
     * @return A quota
     * @deprecated use {@link #list(String, String)} instead.
     */
    @Get("/{quota}")
    @Deprecated(since = "1.12.0")
    public Optional<ResourceQuotaResponse> get(String namespace, String quota) {
        Optional<ResourceQuota> resourceQuota = resourceQuotaService.findByName(namespace, quota);
        if (resourceQuota.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
            resourceQuotaService.getUsedResourcesByQuotaByNamespace(getNamespace(namespace), resourceQuota));
    }

    /**
     * Create a quota.
     *
     * @param namespace The namespace
     * @param quota     The resource quota
     * @param dryrun    Does the creation is a dry run
     * @return The created quota
     */
    @Post("{?dryrun}")
    public HttpResponse<ResourceQuota> apply(String namespace, @Body @Valid ResourceQuota quota,
                                             @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        quota.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        quota.getMetadata().setCluster(ns.getMetadata().getCluster());
        quota.getMetadata().setNamespace(namespace);

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, quota);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(quota, validationErrors);
        }

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findForNamespace(namespace);
        if (resourceQuotaOptional.isPresent() && resourceQuotaOptional.get().equals(quota)) {
            return formatHttpResponse(quota, ApplyStatus.unchanged);
        }

        ApplyStatus status = resourceQuotaOptional.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
        if (dryrun) {
            return formatHttpResponse(quota, status);
        }

        sendEventLog(
            quota,
            status,
            resourceQuotaOptional.<Object>map(ResourceQuota::getSpec).orElse(null),
            quota.getSpec(),
            EMPTY_STRING
        );

        return formatHttpResponse(resourceQuotaService.create(quota), status);
    }

    /**
     * Delete quotas.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @param dryrun    Is dry run mode or not?
     * @return An HTTP response
     */
    @Delete
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> bulkDelete(String namespace, @QueryValue(defaultValue = "*") String name,
                                     @QueryValue(defaultValue = "false") boolean dryrun) {

        List<ResourceQuota> resourceQuotas = resourceQuotaService.findByWildcardName(namespace, name);

        if (resourceQuotas.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        resourceQuotas.forEach(resourceQuota -> {
            sendEventLog(
                resourceQuota,
                ApplyStatus.deleted,
                resourceQuota.getSpec(),
                null,
                EMPTY_STRING
            );
            resourceQuotaService.delete(resourceQuota);
        });

        return HttpResponse.noContent();
    }

    /**
     * Delete a quota.
     *
     * @param namespace The namespace
     * @param name      The resource quota
     * @param dryrun    Is dry run mode or not?
     * @return An HTTP response
     */
    @Delete("/{name}{?dryrun}")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(String namespace, String name,
                                     @QueryValue(defaultValue = "false") boolean dryrun) {
        Optional<ResourceQuota> resourceQuota = resourceQuotaService.findByName(namespace, name);
        if (resourceQuota.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        ResourceQuota resourceQuotaToDelete = resourceQuota.get();

        sendEventLog(
            resourceQuotaToDelete,
            ApplyStatus.deleted,
            resourceQuotaToDelete.getSpec(),
            null,
            EMPTY_STRING
        );

        resourceQuotaService.delete(resourceQuotaToDelete);
        return HttpResponse.noContent();
    }
}
