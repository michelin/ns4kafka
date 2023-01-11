package com.michelin.ns4kafka.controllers.acl;

import com.michelin.ns4kafka.controllers.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import java.util.List;

@Tag(name = "ACLs resource")
@Controller("/api/acls")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class AccessControlListNonNamespacedController extends NonNamespacedResourceController {

    /**
     * The ACL service
     */
    @Inject
    AccessControlEntryService accessControlEntryService;

    /**
     * Get all the ACLs of all namespaces
     * @return A list of ACLs
     */
    @Get
    public List<AccessControlEntry> listAll() {
        return accessControlEntryService.findAll();
    }
}