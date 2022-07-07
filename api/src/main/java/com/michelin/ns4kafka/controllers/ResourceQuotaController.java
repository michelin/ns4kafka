package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Tag(name = "Resource Quota")
@Controller(value = "/api/namespaces/{namespace}/resource-quota")
@ExecuteOn(TaskExecutors.IO)
public class ResourceQuotaController extends NamespacedResourceController {
    /**
     * The resource quota service
     */
    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * Publish a resource quota
     * @param namespace The namespace
     * @param quota The resource quota
     * @param dryrun Does the creation is a dry run
     * @return The created role binding
     */
    @Post("/{?dryrun}")
    HttpResponse<List<ResourceQuotaResponse>> apply(String namespace, @Body @Valid ResourceQuota quota, @QueryValue(defaultValue = "false") boolean dryrun){
        Namespace ns = getNamespace(namespace);

        quota.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        quota.getMetadata().setCluster(ns.getMetadata().getCluster());
        quota.getMetadata().setNamespace(namespace);

        List<String> validationErrors = resourceQuotaService.validateNewQuotaAgainstCurrentResource(ns, quota);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, quota.getKind(), quota.getMetadata().getName());
        }

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByNamespace(namespace);
        if (resourceQuotaOptional.isPresent() && resourceQuotaOptional.get().equals(quota)){
            return formatHttpResponse(resourceQuotaService.mapCurrentQuotaToQuotaResponse(ns), ApplyStatus.unchanged);
        }

        ApplyStatus status = resourceQuotaOptional.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
        if (dryrun) {
            return formatHttpResponse(resourceQuotaService.mapQuotaToQuotaResponse(ns, Optional.of(quota)),
                    status);
        }

        sendEventLog(quota.getKind(),
                quota.getMetadata(),
                status,
                resourceQuotaOptional.<Object>map(ResourceQuota::getSpec).orElse(null),
                quota.getSpec());
        resourceQuotaService.create(quota);
        return formatHttpResponse(resourceQuotaService.mapCurrentQuotaToQuotaResponse(ns), status);
    }
}
