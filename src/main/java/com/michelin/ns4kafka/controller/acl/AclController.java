package com.michelin.ns4kafka.controller.acl;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidAclDeleteOnlyAdmin;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidImmutableField;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNotFound;
import static com.michelin.ns4kafka.util.enumation.Kind.ACCESS_CONTROL_ENTRY;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.AclService;
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
import io.micronaut.security.authentication.Authentication;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Controller to manage ACLs.
 */
@Tag(name = "ACLs", description = "Manage the ACLs.")
@Controller("/api/namespaces/{namespace}/acls")
public class AclController extends NamespacedResourceController {
    @Inject
    AclService aclService;

    /**
     * List ACLs by namespace.
     *
     * @param namespace The namespace
     * @param limit     The ACL scope
     * @return A list of ACLs
     */
    @Get("{?limit}")
    public List<AccessControlEntry> list(String namespace,
                                         Optional<AclLimit> limit,
                                         @QueryValue(defaultValue = "*") String name) {
        Namespace ns = getNamespace(namespace);
        return switch (limit.orElse(AclLimit.ALL)) {
            case GRANTEE -> aclService.findAllGrantedToNamespaceByWildcardName(ns, name)
                .stream()
                .sorted(Comparator.comparing((AccessControlEntry acl) -> acl.getMetadata().getNamespace()))
                .toList();
            case GRANTOR -> aclService.findAllGrantedByNamespaceByWildcardName(ns, name)
                .stream()
                .sorted(Comparator.comparing(acl -> acl.getSpec().getGrantedTo()))
                .toList();
            default -> aclService.findAllRelatedToNamespaceByWildcardName(ns, name)
                .stream()
                .sorted(Comparator
                    .comparing((AccessControlEntry acl) -> acl.getMetadata().getNamespace())
                    .thenComparing(acl -> acl.getSpec().getGrantedTo()))
                .toList();
        };
    }

    /**
     * Get an ACL by namespace and name.
     *
     * @param namespace The name
     * @param acl   The ACL name
     * @return The ACL
     * @deprecated use list(String, Optional ALL, String name) instead.
     */
    @Get("/{acl}")
    @Deprecated(since = "1.12.0")
    public Optional<AccessControlEntry> get(String namespace, String acl) {
        return aclService.findAllRelatedToNamespace(getNamespace(namespace))
            .stream()
            .filter(accessControlEntry -> accessControlEntry.getMetadata().getName().equals(acl))
            .findFirst();
    }

    /**
     * Create an ACL.
     *
     * @param authentication     The authentication entity
     * @param namespace          The namespace
     * @param accessControlEntry The ACL
     * @param dryrun             Is dry run mode or not ?
     * @return An HTTP response
     */
    @Post("{?dryrun}")
    public HttpResponse<AccessControlEntry> apply(Authentication authentication, String namespace,
                                                  @Valid @Body AccessControlEntry accessControlEntry,
                                                  @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        boolean isAdmin = authentication.getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN);
        boolean isSelfAssigned = namespace.equals(accessControlEntry.getSpec().getGrantedTo());

        List<String> validationErrors;
        if (isAdmin && isSelfAssigned) {
            // Validate overlapping OWNER
            validationErrors = aclService.validateAsAdmin(accessControlEntry, ns);
        } else {
            validationErrors = aclService.validate(accessControlEntry, ns);
        }

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(accessControlEntry, validationErrors);
        }

        // AccessControlEntry spec is immutable
        // This prevents accidental updates on ACL resources already declared with the same name (with different rules)
        Optional<AccessControlEntry> existingAcl =
            aclService.findByName(namespace, accessControlEntry.getMetadata().getName());
        if (existingAcl.isPresent() && !existingAcl.get().getSpec().equals(accessControlEntry.getSpec())) {
            throw new ResourceValidationException(accessControlEntry,
                invalidImmutableField("spec"));
        }

        accessControlEntry.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        accessControlEntry.getMetadata().setCluster(ns.getMetadata().getCluster());
        accessControlEntry.getMetadata().setNamespace(ns.getMetadata().getName());

        if (existingAcl.isPresent() && existingAcl.get().equals(accessControlEntry)) {
            return formatHttpResponse(existingAcl.get(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingAcl.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        if (dryrun) {
            return formatHttpResponse(accessControlEntry, status);
        }

        sendEventLog(accessControlEntry, status, existingAcl.<Object>map(AccessControlEntry::getSpec).orElse(null),
            accessControlEntry.getSpec());

        return formatHttpResponse(aclService.create(accessControlEntry), status);
    }

    /**
     * Delete an ACL.
     *
     * @param authentication The authentication entity
     * @param namespace      The namespace
     * @param name           The ACL name
     * @param dryrun         Is dry run mode or not ?
     * @return An HTTP response
     */
    @Delete("/{name}{?dryrun}")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(Authentication authentication, String namespace, String name,
                                     @QueryValue(defaultValue = "false") boolean dryrun) {
        AccessControlEntry accessControlEntry = aclService
            .findByName(namespace, name)
            .orElseThrow(() -> new ResourceValidationException(ACCESS_CONTROL_ENTRY, name, invalidNotFound(name)));

        boolean isAdmin = authentication.getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN);
        boolean isSelfAssignedAcl = namespace.equals(accessControlEntry.getSpec().getGrantedTo());

        if (isSelfAssignedAcl && !isAdmin) {
            throw new ResourceValidationException(ACCESS_CONTROL_ENTRY, name, invalidAclDeleteOnlyAdmin(name));
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        sendEventLog(accessControlEntry, ApplyStatus.deleted, accessControlEntry.getSpec(), null);

        aclService.delete(accessControlEntry);
        return HttpResponse.noContent();
    }

    /**
     * ACL scope.
     */
    public enum AclLimit {
        /**
         * Returns all ACL scopes.
         */
        ALL,
        GRANTOR,
        GRANTEE
    }
}
