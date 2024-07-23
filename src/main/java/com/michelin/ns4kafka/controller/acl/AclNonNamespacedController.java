package com.michelin.ns4kafka.controller.acl;

import com.michelin.ns4kafka.controller.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.AclService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import java.util.List;

/**
 * Non-namespaced controller to manage ACLs.
 */
@Tag(name = "ACLs", description = "Manage the ACLs.")
@Controller("/api/acls")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class AclNonNamespacedController extends NonNamespacedResourceController {
    @Inject
    AclService aclService;

    /**
     * List ACLs.
     *
     * @return A list of ACLs
     */
    @Get
    public List<AccessControlEntry> listAll() {
        return aclService.findAll();
    }
}
