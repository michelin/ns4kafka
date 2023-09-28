package com.michelin.ns4kafka.controllers.quota;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;

import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Tag(name = "Quotas", description = "Manage the resource quotas.")
@Controller(value = "/api/namespaces/{namespace}/resource-quotas")
@ExecuteOn(TaskExecutors.IO)
public class ResourceQuotaController extends NamespacedResourceController {
    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * List quotas by namespace
     * @param namespace The namespace
     * @return A list of quotas
     */
    @Get
    public List<ResourceQuotaResponse> list(String namespace) {
        Namespace ns = getNamespace(namespace);
        return List.of(resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns,
                resourceQuotaService.findByNamespace(namespace)));
    }

    /**
     * Get a quota by namespace and name
     * @param namespace The name
     * @param quota The quota name
     * @return A quota
     */
    @Get("/{quota}")
    public Optional<ResourceQuotaResponse> get(String namespace, String quota) {
        Namespace ns = getNamespace(namespace);
        Optional<ResourceQuota> resourceQuota = resourceQuotaService.findByName(namespace, quota);
        if (resourceQuota.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns, resourceQuota));
    }

    /**
     * Create a quota
     * @param namespace The namespace
     * @param quota The resource quota
     * @param dryrun Does the creation is a dry run
     * @return The created quota
     */
    @Post("{?dryrun}")
    public HttpResponse<ResourceQuota> apply(String namespace, @Body @Valid ResourceQuota quota, @QueryValue(defaultValue = "false") boolean dryrun){
        Namespace ns = getNamespace(namespace);

        quota.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        quota.getMetadata().setCluster(ns.getMetadata().getCluster());
        quota.getMetadata().setNamespace(namespace);

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, quota);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, quota.getKind(), quota.getMetadata().getName());
        }

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByNamespace(namespace);
        if (resourceQuotaOptional.isPresent() && resourceQuotaOptional.get().equals(quota)) {
            return formatHttpResponse(quota, ApplyStatus.unchanged);
        }

        ApplyStatus status = resourceQuotaOptional.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
        if (dryrun) {
            return formatHttpResponse(quota, status);
        }

        sendEventLog(quota.getKind(), quota.getMetadata(), status,
                resourceQuotaOptional.<Object>map(ResourceQuota::getSpec).orElse(null), quota.getSpec());

        return formatHttpResponse(resourceQuotaService.create(quota), status);
    }

    /**
     * Delete a quota
     * @param namespace The namespace
     * @param name The resource quota
     * @param dryrun Is dry run mode or not ?
     * @return An HTTP response
     */
    @Delete("/{name}{?dryrun}")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(String namespace, String name, @QueryValue(defaultValue = "false") boolean dryrun) {
        Optional<ResourceQuota> resourceQuota = resourceQuotaService.findByName(namespace, name);
        if (resourceQuota.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        ResourceQuota resourceQuotaToDelete = resourceQuota.get();
        sendEventLog(resourceQuotaToDelete .getKind(), resourceQuotaToDelete.getMetadata(), ApplyStatus.deleted,
                resourceQuotaToDelete.getSpec(), null);
        resourceQuotaService.delete(resourceQuotaToDelete);
        return HttpResponse.noContent();
    }
}
