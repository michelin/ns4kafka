/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.controller.acl;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidAclDeleteOnlyAdmin;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidImmutableField;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNotFound;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidSelfAssignedAclDelete;
import static com.michelin.ns4kafka.util.enumation.Kind.ACCESS_CONTROL_ENTRY;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

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

/** Controller to manage ACLs. */
@Tag(name = "ACLs", description = "Manage the ACLs.")
@Controller("/api/namespaces/{namespace}/acls")
public class AclController extends NamespacedResourceController {
    @Inject
    private AclService aclService;

    /**
     * List ACLs by namespace.
     *
     * @param namespace The namespace
     * @param limit The ACL scope
     * @return A list of ACLs
     */
    @Get("{?limit}")
    public List<AccessControlEntry> list(
            String namespace, Optional<AclLimit> limit, @QueryValue(defaultValue = "*") String name) {
        Namespace ns = getNamespace(namespace);
        return switch (limit.orElse(AclLimit.ALL)) {
            case GRANTEE ->
                aclService.findAllGrantedToNamespaceByWildcardName(ns, name).stream()
                        .sorted(Comparator.comparing(
                                (AccessControlEntry acl) -> acl.getMetadata().getNamespace()))
                        .toList();
            case GRANTOR ->
                aclService.findAllGrantedByNamespaceToOthersByWildcardName(ns, name).stream()
                        .sorted(Comparator.comparing(acl -> acl.getSpec().getGrantedTo()))
                        .toList();
            default ->
                aclService.findAllRelatedToNamespaceByWildcardName(ns, name).stream()
                        .sorted(Comparator.comparing((AccessControlEntry acl) ->
                                        acl.getMetadata().getNamespace())
                                .thenComparing(acl -> acl.getSpec().getGrantedTo()))
                        .toList();
        };
    }

    /**
     * Get an ACL by namespace and name.
     *
     * @param namespace The name
     * @param acl The ACL name
     * @return The ACL
     * @deprecated use {@link #list(String, Optional, String)} instead.
     */
    @Get("/{acl}")
    @Deprecated(since = "1.12.0")
    public Optional<AccessControlEntry> get(String namespace, String acl) {
        return aclService.findAllRelatedToNamespace(getNamespace(namespace)).stream()
                .filter(accessControlEntry ->
                        accessControlEntry.getMetadata().getName().equals(acl))
                .findFirst();
    }

    /**
     * Create an ACL.
     *
     * @param authentication The authentication entity
     * @param namespace The namespace
     * @param accessControlEntry The ACL
     * @param dryrun Is dry run mode or not?
     * @return An HTTP response
     */
    @Post("{?dryrun}")
    public HttpResponse<AccessControlEntry> apply(
            Authentication authentication,
            String namespace,
            @Valid @Body AccessControlEntry accessControlEntry,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        boolean isAdmin = authentication.getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN);
        boolean isSelfAssigned = namespace.equals(accessControlEntry.getSpec().getGrantedTo());

        List<String> validationErrors;

        if (isAdmin && isSelfAssigned) {
            // Validate overlapping OWNER
            validationErrors = aclService.validateSelfAssignedAdmin(accessControlEntry, ns);
        } else {
            validationErrors = aclService.validate(accessControlEntry, ns);
        }

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(accessControlEntry, validationErrors);
        }

        // AccessControlEntry spec is immutable
        // This prevents accidental updates on ACL resources already declared with the same name (with different rules)
        Optional<AccessControlEntry> existingAcl = aclService.findByName(
                namespace, accessControlEntry.getMetadata().getName());
        if (existingAcl.isPresent() && !existingAcl.get().getSpec().equals(accessControlEntry.getSpec())) {
            throw new ResourceValidationException(accessControlEntry, invalidImmutableField("spec"));
        }

        accessControlEntry.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        accessControlEntry.getMetadata().setCluster(ns.getMetadata().getCluster());
        accessControlEntry.getMetadata().setNamespace(ns.getMetadata().getName());

        if (existingAcl.isPresent() && existingAcl.get().equals(accessControlEntry)) {
            return formatHttpResponse(existingAcl.get(), ApplyStatus.UNCHANGED);
        }

        ApplyStatus status = existingAcl.isPresent() ? ApplyStatus.CHANGED : ApplyStatus.CREATED;

        if (dryrun) {
            return formatHttpResponse(accessControlEntry, status);
        }

        sendEventLog(
                accessControlEntry,
                status,
                existingAcl.<Object>map(AccessControlEntry::getSpec).orElse(null),
                accessControlEntry.getSpec(),
                EMPTY_STRING);

        return formatHttpResponse(aclService.create(accessControlEntry), status);
    }

    /**
     * Delete an ACL.
     *
     * @param authentication The authentication entity
     * @param namespace The namespace
     * @param name The ACL name parameter
     * @param dryrun Is dry run mode or not?
     * @return An HTTP response
     */
    @Delete
    @Status(HttpStatus.OK)
    public HttpResponse<List<AccessControlEntry>> bulkDelete(
            Authentication authentication,
            String namespace,
            @QueryValue(defaultValue = "*") String name,
            @QueryValue(defaultValue = "false") boolean dryrun) {

        Namespace ns = getNamespace(namespace);
        List<AccessControlEntry> acls = aclService.findAllGrantedByNamespaceByWildcardName(ns, name);
        List<AccessControlEntry> selfAssignedAcls = acls.stream()
                .filter(acl -> namespace.equals(acl.getSpec().getGrantedTo()))
                .toList();
        boolean isAdmin = authentication.getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN);

        if (acls.isEmpty()) {
            return HttpResponse.notFound();
        }

        // If non-admin tries to delete at least one self-assigned ACL, throw validation error
        if (!isAdmin && !selfAssignedAcls.isEmpty()) {
            List<String> selfAssignedAclsNames = selfAssignedAcls.stream()
                    .map(acl -> acl.getMetadata().getName())
                    .toList();
            throw new ResourceValidationException(
                    ACCESS_CONTROL_ENTRY,
                    name,
                    invalidSelfAssignedAclDelete(name, String.join(", ", selfAssignedAclsNames)));
        }

        if (dryrun) {
            return HttpResponse.ok(acls);
        }

        acls.forEach(acl -> {
            sendEventLog(acl, ApplyStatus.DELETED, acl.getSpec(), null, EMPTY_STRING);
            aclService.delete(acl);
        });

        return HttpResponse.ok(acls);
    }

    /**
     * Delete an ACL.
     *
     * @param authentication The authentication entity
     * @param namespace The namespace
     * @param name The ACL name
     * @param dryrun Is dry run mode or not?
     * @return An HTTP response
     * @deprecated use {@link #bulkDelete(Authentication, String, String, boolean)} instead.
     */
    @Delete("/{name}{?dryrun}")
    @Deprecated(since = "1.13.0")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(
            Authentication authentication,
            String namespace,
            String name,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        AccessControlEntry accessControlEntry = aclService
                .findByName(namespace, name)
                .orElseThrow(() -> new ResourceValidationException(ACCESS_CONTROL_ENTRY, name, invalidNotFound(name)));

        boolean isAdmin = authentication.getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN);
        boolean isSelfAssignedAcl =
                namespace.equals(accessControlEntry.getSpec().getGrantedTo());

        if (isSelfAssignedAcl && !isAdmin) {
            throw new ResourceValidationException(ACCESS_CONTROL_ENTRY, name, invalidAclDeleteOnlyAdmin(name));
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        sendEventLog(accessControlEntry, ApplyStatus.DELETED, accessControlEntry.getSpec(), null, EMPTY_STRING);

        aclService.delete(accessControlEntry);
        return HttpResponse.noContent();
    }

    /** ACL scope. */
    public enum AclLimit {
        /** Returns all ACL scopes. */
        ALL,
        GRANTOR,
        GRANTEE
    }
}
