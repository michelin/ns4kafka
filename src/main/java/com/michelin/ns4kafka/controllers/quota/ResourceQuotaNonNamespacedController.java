package com.michelin.ns4kafka.controllers.quota;

import com.michelin.ns4kafka.controllers.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;

import javax.annotation.security.RolesAllowed;
import java.util.List;

@Tag(name = "Resource Quotas")
@Controller(value = "/api/resource-quotas")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class ResourceQuotaNonNamespacedController extends NonNamespacedResourceController {
    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * Get all the quotas of all namespaces
     * @return A list of quotas
     */
    @Get
    public List<ResourceQuotaResponse> listAll() {
        return resourceQuotaService.getUsedResourcesByQuotaForAllNamespaces();
    }
}
