package com.michelin.ns4kafka.controllers.acl;

import static com.michelin.ns4kafka.services.AccessControlEntryService.PUBLIC_GRANTED_TO;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
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
    AccessControlEntryService accessControlEntryService;

    /**
     * List ACLs by namespace.
     *
     * @param namespace The namespace
     * @param limit     The ACL scope
     * @return A list of ACLs
     */
    @Get("{?limit}")
    public List<AccessControlEntry> list(String namespace, Optional<AclLimit> limit) {
        if (limit.isEmpty()) {
            limit = Optional.of(AclLimit.ALL);
        }

        Namespace ns = getNamespace(namespace);
        return switch (limit.get()) {
            case GRANTEE -> accessControlEntryService.findAllGrantedToNamespace(ns)
                .stream()
                .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                .toList();
            case GRANTOR -> accessControlEntryService.findAllForCluster(ns.getMetadata().getCluster())
                .stream()
                // granted by me
                .filter(accessControlEntry -> accessControlEntry.getMetadata().getNamespace().equals(namespace))
                // without the granted to me
                .filter(accessControlEntry -> !accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                .sorted(Comparator.comparing(o -> o.getSpec().getGrantedTo()))
                .toList();
            default -> accessControlEntryService.findAllForCluster(ns.getMetadata().getCluster())
                .stream()
                .filter(accessControlEntry ->
                    accessControlEntry.getMetadata().getNamespace().equals(namespace)
                        || accessControlEntry.getSpec().getGrantedTo().equals(namespace)
                        || accessControlEntry.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO))
                .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                .toList();
        };
    }

    /**
     * Get an ACL by namespace and name.
     *
     * @param namespace The name
     * @param acl       The ACL name
     * @return The ACL
     */
    @Get("/{acl}")
    public Optional<AccessControlEntry> get(String namespace, String acl) {
        return list(namespace, Optional.of(AclLimit.ALL))
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

        List<String> roles = (List<String>) authentication.getAttributes().get("roles");
        boolean isAdmin = roles.contains(ResourceBasedSecurityRule.IS_ADMIN);
        boolean isSelfAssignedAcl = namespace.equals(accessControlEntry.getSpec().getGrantedTo());

        List<String> validationErrors;
        if (isAdmin && isSelfAssignedAcl) {
            // Validate overlapping OWNER
            validationErrors = accessControlEntryService.validateAsAdmin(accessControlEntry, ns);
        } else {
            validationErrors = accessControlEntryService.validate(accessControlEntry, ns);
        }

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, accessControlEntry.getKind(),
                accessControlEntry.getMetadata().getName());
        }

        // AccessControlEntry spec is immutable
        // This prevents accidental updates on ACL resources already declared with the same name (with different rules)
        Optional<AccessControlEntry> existingAcl =
            accessControlEntryService.findByName(namespace, accessControlEntry.getMetadata().getName());
        if (existingAcl.isPresent() && !existingAcl.get().getSpec().equals(accessControlEntry.getSpec())) {
            throw new ResourceValidationException(
                List.of("Invalid modification: `spec` is immutable. You can still update `metadata`"),
                accessControlEntry.getKind(), accessControlEntry.getMetadata().getName());
        }

        accessControlEntry.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        accessControlEntry.getMetadata().setCluster(ns.getMetadata().getCluster());
        accessControlEntry.getMetadata().setNamespace(ns.getMetadata().getName());

        if (existingAcl.isPresent() && existingAcl.get().equals(accessControlEntry)) {
            return formatHttpResponse(existingAcl.get(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingAcl.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        // Dry run checks
        if (dryrun) {
            return formatHttpResponse(accessControlEntry, status);
        }

        sendEventLog(accessControlEntry.getKind(),
            accessControlEntry.getMetadata(),
            status,
            existingAcl.<Object>map(AccessControlEntry::getSpec).orElse(null),
            accessControlEntry.getSpec());

        // Store
        return formatHttpResponse(accessControlEntryService.create(accessControlEntry), status);
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
        AccessControlEntry accessControlEntry = accessControlEntryService
            .findByName(namespace, name)
            .orElseThrow(() -> new ResourceValidationException(
                List.of("Invalid value " + name + " for name: ACL does not exist in this namespace."),
                "AccessControlEntry", name));

        List<String> roles = (List<String>) authentication.getAttributes().get("roles");
        boolean isAdmin = roles.contains(ResourceBasedSecurityRule.IS_ADMIN);
        boolean isSelfAssignedAcl = namespace.equals(accessControlEntry.getSpec().getGrantedTo());

        if (isSelfAssignedAcl && !isAdmin) {
            throw new ResourceValidationException(List.of("Only admins can delete this ACL."), "AccessControlEntry",
                name);
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        sendEventLog(accessControlEntry.getKind(), accessControlEntry.getMetadata(), ApplyStatus.deleted,
            accessControlEntry.getSpec(), null);
        accessControlEntryService.delete(getNamespace(namespace), accessControlEntry);
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
