package com.michelin.ns4kafka.controller.quota;

import com.michelin.ns4kafka.controller.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.model.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.ResourceQuotaService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import java.util.List;

/**
 * Non namespaced resource quota controller.
 */
@Tag(name = "Quotas", description = "Manage the resource quotas.")
@Controller(value = "/api/resource-quotas")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class ResourceQuotaNonNamespacedController extends NonNamespacedResourceController {
    @Inject
    ResourceQuotaService resourceQuotaService;

    @Inject
    NamespaceService namespaceService;

    /**
     * List quotas.
     *
     * @return A list of quotas
     */
    @Get
    public List<ResourceQuotaResponse> listAll() {
        return resourceQuotaService.getUsedQuotaByNamespaces(namespaceService.listAll());
    }
}
