package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.NamespaceService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.security.authentication.Authentication;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Tag(name = "Cross Namespace Topic Grants",
        description = "APIs to handle cross namespace ACL")
@Controller("/api/namespaces/{namespace}/acls")
public class AccessControlListController extends NamespacedResourceController {
    @Inject
    NamespaceService namespaceService;
    @Inject
    AccessControlEntryService accessControlEntryService;

    @Operation(summary = "Returns the Access Control Entry List")
    @Get("{?limit}")
    public List<AccessControlEntry> list(String namespace, Optional<AclLimit> limit) {
        if (limit.isEmpty())
            limit = Optional.of(AclLimit.ALL);

        Namespace ns = getNamespace(namespace);

        switch (limit.get()) {
            case GRANTEE:
                return accessControlEntryService.findAllGrantedToNamespace(ns)
                        .stream()
                        // granted to me
                        .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                        .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                        .collect(Collectors.toList());
            case GRANTOR:
                return accessControlEntryService.findAllForCluster(ns.getMetadata().getCluster())
                        .stream()
                        // granted by me
                        .filter(accessControlEntry -> accessControlEntry.getMetadata().getNamespace().equals(namespace))
                        // without the granted to me
                        .filter(accessControlEntry -> !accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                        .sorted(Comparator.comparing(o -> o.getSpec().getGrantedTo()))
                        .collect(Collectors.toList());
            case ALL:
            default:
                return accessControlEntryService.findAllForCluster(ns.getMetadata().getCluster())
                        .stream()
                        .filter(accessControlEntry ->
                                accessControlEntry.getMetadata().getNamespace().equals(namespace)
                                        || accessControlEntry.getSpec().getGrantedTo().equals(namespace)
                        )
                        .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                        .collect(Collectors.toList());
        }

    }

    @Post("{?dryrun}")
    public AccessControlEntry apply(Authentication authentication, String namespace, @Valid @Body AccessControlEntry accessControlEntry, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        List<String> roles = (List<String>) authentication.getAttributes().get("roles");
        boolean isAdmin = roles.contains(ResourceBasedSecurityRule.IS_ADMIN);
        // self assigned ACL (spec.grantedTo == metadata.namespace) with permission OWNER
        boolean isSelfAssignedOwnerACL = namespace.equals(accessControlEntry.getSpec().getGrantedTo())
                && accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER;

        List<String> validationErrors;
        if (isAdmin && isSelfAssignedOwnerACL) {
            // validate overlapping OWNER
            validationErrors = accessControlEntryService.validateAsAdmin(accessControlEntry, ns);
        } else {
            validationErrors = accessControlEntryService.validate(accessControlEntry, ns);
        }
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        //augment
        accessControlEntry.getMetadata().setCluster(ns.getMetadata().getCluster());
        accessControlEntry.getMetadata().setNamespace(ns.getMetadata().getName());
        //dryrun checks
        if (dryrun) {
            return accessControlEntry;
        }
        //store
        return accessControlEntryService.create(accessControlEntry);
    }

    @Delete("/{name}{?dryrun}")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(Authentication authentication, String namespace, String name, @QueryValue(defaultValue = "false") boolean dryrun) {

        AccessControlEntry accessControlEntry = accessControlEntryService
                .findByName(namespace, name)
                .orElseThrow(() -> new ResourceValidationException(
                        List.of("Invalid value " + name + " for name : AccessControlEntry doesn't exist in this namespace"))
                );

        List<String> roles = (List<String>) authentication.getAttributes().get("roles");
        boolean isAdmin = roles.contains(ResourceBasedSecurityRule.IS_ADMIN);
        // self assigned ACL (spec.grantedTo == metadata.namespace) with permission OWNER
        boolean isSelfAssignedACL = namespace.equals(accessControlEntry.getSpec().getGrantedTo());
        if (isSelfAssignedACL && !isAdmin) {
            // prevent delete
            throw new ResourceValidationException(List.of("Only admins can delete this AccessControlEntry"));
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        accessControlEntryService.delete(accessControlEntry);
        return HttpResponse.noContent();
    }

    public enum AclLimit {
        /**
         * Returns all ACL
         */
        ALL,
        GRANTOR,
        GRANTEE
    }
}
