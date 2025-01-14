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

package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidAclCollision;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidAclGrantedToMyself;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidAclNotOwnerOfTopLevel;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidAclPatternType;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidAclPermission;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidAclResourceType;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNotFound;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
import com.michelin.ns4kafka.service.executor.AccessControlEntryAsyncExecutor;
import com.michelin.ns4kafka.util.RegexUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Access control entry service.
 */
@Singleton
public class AclService {
    public static final String PUBLIC_GRANTED_TO = "*";

    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    @Inject
    ApplicationContext applicationContext;

    /**
     * Validate a new ACL.
     *
     * @param accessControlEntry The ACL
     * @param namespace          The namespace
     * @return A list of validation errors
     */
    public List<String> validate(AccessControlEntry accessControlEntry, Namespace namespace) {
        List<String> validationErrors = new ArrayList<>();

        // Which resource can be granted cross namespaces
        List<AccessControlEntry.ResourceType> allowedResourceTypes =
            List.of(AccessControlEntry.ResourceType.TOPIC, AccessControlEntry.ResourceType.CONNECT_CLUSTER);

        // Which permission can be granted cross namespaces ? READ, WRITE
        // Only admin can grant OWNER
        List<AccessControlEntry.Permission> allowedPermissions =
            List.of(AccessControlEntry.Permission.READ,
                AccessControlEntry.Permission.WRITE);

        // Which patternTypes can be granted
        List<AccessControlEntry.ResourcePatternType> allowedPatternTypes =
            List.of(AccessControlEntry.ResourcePatternType.LITERAL,
                AccessControlEntry.ResourcePatternType.PREFIXED);

        if (!allowedResourceTypes.contains(accessControlEntry.getSpec().getResourceType())) {
            validationErrors.add(invalidAclResourceType(
                String.valueOf(accessControlEntry.getSpec().getResourceType()),
                allowedResourceTypes.stream().map(Object::toString).collect(Collectors.joining(", "))));
        }

        if (!allowedPermissions.contains(accessControlEntry.getSpec().getPermission())) {
            validationErrors.add(invalidAclPermission(String.valueOf(accessControlEntry.getSpec().getPermission()),
                allowedPermissions.stream().map(Object::toString).collect(Collectors.joining(", "))));
        }

        if (!allowedPatternTypes.contains(accessControlEntry.getSpec().getResourcePatternType())) {
            validationErrors.add(invalidAclPatternType(
                String.valueOf(accessControlEntry.getSpec().getResourcePatternType()),
                allowedPatternTypes.stream().map(Object::toString).collect(Collectors.joining(", "))));
        }

        // GrantedTo Namespace exists ?
        NamespaceService namespaceService = applicationContext.getBean(NamespaceService.class);
        Optional<Namespace> grantedToNamespace =
            namespaceService.findByName(accessControlEntry.getSpec().getGrantedTo());
        if (grantedToNamespace.isEmpty() && !accessControlEntry.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO)) {
            validationErrors.add(invalidNotFound("grantedTo", accessControlEntry.getSpec().getGrantedTo()));
        }

        if (namespace.getMetadata().getName().equals(accessControlEntry.getSpec().getGrantedTo())) {
            validationErrors.add(invalidAclGrantedToMyself(accessControlEntry.getSpec().getGrantedTo()));
        }

        if (!isOwnerOfTopLevelAcl(accessControlEntry, namespace)) {
            validationErrors.add(invalidAclNotOwnerOfTopLevel(accessControlEntry.getSpec().getResource(),
                accessControlEntry.getSpec().getResourcePatternType()));
        }

        return validationErrors;
    }

    /**
     * Validate a new ACL created by an admin.
     *
     * @param accessControlEntry The ACL
     * @param namespace          The namespace
     * @return A list of validation errors
     */
    public List<String> validateAsAdmin(AccessControlEntry accessControlEntry, Namespace namespace) {
        // another namespace is already OWNER of PREFIXED or LITERAL resource
        // example :
        // if already exists:
        //   namespace1 OWNER:PREFIXED:project1
        //   namespace1 OWNER:LITERAL:project2_t1
        // and we try to create:
        //   namespace2 OWNER:PREFIXED:project1             KO 1 same
        //   namespace2 OWNER:LITERAL:project1              KO 2 same
        //   namespace2 OWNER:PREFIXED:project1_sub         KO 3 child overlap
        //   namespace2 OWNER:LITERAL:project1_t1           KO 4 child overlap
        //   namespace2 OWNER:PREFIXED:proj                 KO 5 parent overlap
        //   namespace2 OWNER:PREFIXED:project2             KO 6 parent overlap
        //
        //   namespace2 OWNER:PREFIXED:project3_topic1_sub  OK 7
        //   namespace2 OWNER:PREFIXED:project2             OK 8
        //   namespace2 OWNER:LITERAL:proj                  OK 9
        return findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            // Do not include the ACL if it is itself
            .filter(ace -> !ace.getMetadata().getNamespace().equals(namespace.getMetadata().getName())
                || !ace.getMetadata().getName().equals(accessControlEntry.getMetadata().getName()))
            .filter(ace -> ace.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
            .filter(ace -> ace.getSpec().getResourceType() == accessControlEntry.getSpec().getResourceType())
            .filter(ace -> {
                // new PREFIXED ACL would cover existing ACLs
                boolean parentOverlap = false;
                if (accessControlEntry.getSpec().getResourcePatternType()
                    == AccessControlEntry.ResourcePatternType.PREFIXED) {
                    parentOverlap = ace.getSpec().getResource().startsWith(accessControlEntry.getSpec().getResource())
                        || topicAclsCollideWithParentOrChild(ace, accessControlEntry);
                }

                // new ACL would be covered by a PREFIXED existing ACLs
                boolean childOverlap = false;
                if (ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED) {
                    childOverlap = accessControlEntry.getSpec().getResource().startsWith(ace.getSpec().getResource())
                        || topicAclsCollideWithParentOrChild(accessControlEntry, ace);
                }

                boolean same = accessControlEntry.getSpec().getResource().equals(ace.getSpec().getResource())
                    || topicAclsCollide(accessControlEntry, ace);

                return same || parentOverlap || childOverlap;
            })
            .map(ace -> invalidAclCollision(accessControlEntry.getMetadata().getName(), ace.getMetadata().getName()))
            .toList();
    }

    /**
     * Check if two topic ACLs have a collision with parent or child.
     *
     * @param topicAclA The first ACL
     * @param topicAclB The second ACL
     * @return true if they have a collision, false otherwise
     */
    public boolean topicAclsCollideWithParentOrChild(AccessControlEntry topicAclA, AccessControlEntry topicAclB) {
        return topicAclA.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.TOPIC)
            && topicAclB.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.TOPIC)
            && topicAclA.getSpec().getResource().replace('.', '_')
            .startsWith(topicAclB.getSpec().getResource().replace('.', '_'));
    }

    /**
     * Check if two topic ACLs have a collision.
     *
     * @param topicAclA The first ACL
     * @param topicAclB The second ACL
     * @return true if they have a collision, false otherwise
     */
    public boolean topicAclsCollide(AccessControlEntry topicAclA, AccessControlEntry topicAclB) {
        return topicAclA.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.TOPIC)
            && topicAclB.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.TOPIC)
            && topicAclA.getSpec().getResource().replace('.', '_')
            .equals(topicAclB.getSpec().getResource().replace('.', '_'));
    }

    /**
     * Is namespace owner of given ACL.
     *
     * @param accessControlEntry The ACL
     * @param namespace          The namespace
     * @return true if it is, false otherwise
     */
    public boolean isOwnerOfTopLevelAcl(AccessControlEntry accessControlEntry, Namespace namespace) {
        // Grantor Namespace is OWNER of Resource + ResourcePattern ?
        return findAllGrantedToNamespace(namespace)
            .stream()
            .filter(ace -> ace.getSpec().getResourceType() == accessControlEntry.getSpec().getResourceType()
                && ace.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
            .anyMatch(ace -> {
                // if grantor is owner of PREFIXED resource that starts with
                // owner  PREFIXED: priv_bsm_
                // grants LITERAL : priv_bsm_topic  OK
                // grants PREFIXED: priv_bsm_topic  OK
                // grants PREFIXED: priv_b          NO
                // grants LITERAL : priv_b          NO
                // grants PREFIXED: priv_bsm_       OK
                // grants LITERAL : pric_bsm_       OK
                if (ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED
                    && accessControlEntry.getSpec().getResource().startsWith(ace.getSpec().getResource())) {
                    // if so, either patternType are fine (LITERAL/PREFIXED)
                    return true;
                }
                // if grantor is owner of LITERAL resource :
                // exact match to LITERAL grant
                // owner  LITERAL : priv_bsm_topic
                // grants LITERAL : priv_bsm_topic  OK
                // grants PREFIXED: priv_bsm_topic  NO
                // grants PREFIXED: priv_bs         NO
                // grants LITERAL : priv_b          NO
                // grants PREFIXED: priv_bsm_topic2 NO
                // grants LITERAL : pric_bsm_topic2 NO
                return ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL
                    && accessControlEntry.getSpec().getResourcePatternType()
                    == AccessControlEntry.ResourcePatternType.LITERAL
                    && accessControlEntry.getSpec().getResource().equals(ace.getSpec().getResource());
            });
    }

    /**
     * Create an ACL in internal topic.
     *
     * @param accessControlEntry The ACL
     * @return The created ACL
     */
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        return accessControlEntryRepository.create(accessControlEntry);
    }

    /**
     * Delete an ACL from broker and from internal topic.
     *
     * @param accessControlEntry The ACL
     */
    public void delete(AccessControlEntry accessControlEntry) {
        AccessControlEntryAsyncExecutor accessControlEntryAsyncExecutor =
            applicationContext.getBean(AccessControlEntryAsyncExecutor.class,
                Qualifiers.byName(accessControlEntry.getMetadata().getCluster()));
        accessControlEntryAsyncExecutor.deleteAcl(accessControlEntry);

        accessControlEntryRepository.delete(accessControlEntry);
    }

    /**
     * Find all ACLs granted to a given namespace.
     * Will also return public granted ACLs.
     *
     * @param namespace The namespace
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllGrantedToNamespace(Namespace namespace) {
        return findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(acl -> acl.getSpec().getGrantedTo().equals(namespace.getMetadata().getName())
                || acl.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO))
            .toList();
    }

    /**
     * Find all ACLs granted by a given namespace.
     *
     * @param namespace The namespace
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllGrantedByNamespace(Namespace namespace) {
        return findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(acl -> acl.getMetadata().getNamespace().equals(namespace.getMetadata().getName()))
            .toList();
    }

    /**
     * Find all ACLs that a given namespace granted to other namespaces.
     *
     * @param namespace The namespace
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllGrantedByNamespaceToOthers(Namespace namespace) {
        return findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(acl -> acl.getMetadata().getNamespace().equals(namespace.getMetadata().getName()))
            .filter(acl -> !acl.getSpec().getGrantedTo().equals(namespace.getMetadata().getName()))
            .toList();
    }

    /**
     * Find all ACLs where the given namespace is either the grantor or the grantee, or the ACL is public.
     *
     * @param namespace The namespace
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllRelatedToNamespace(Namespace namespace) {
        return findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(acl -> acl.getMetadata().getNamespace().equals(namespace.getMetadata().getName())
                || acl.getSpec().getGrantedTo().equals(namespace.getMetadata().getName())
                || acl.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO))
            .toList();
    }

    /**
     * Find all ACLs granted to given namespace, filtered by name parameter.
     * Will also return public granted ACLs.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllGrantedToNamespaceByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAllGrantedToNamespace(namespace)
            .stream()
            .filter(acl -> RegexUtils.isResourceCoveredByRegex(acl.getMetadata().getName(), nameFilterPatterns))
            .toList();
    }

    /**
     * Find all ACLs granted by a given namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllGrantedByNamespaceByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAllGrantedByNamespace(namespace)
            .stream()
            .filter(acl -> RegexUtils.isResourceCoveredByRegex(acl.getMetadata().getName(), nameFilterPatterns))
            .toList();
    }

    /**
     * Find all ACLs that a given namespace granted to other namespaces, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllGrantedByNamespaceToOthersByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAllGrantedByNamespaceToOthers(namespace)
            .stream()
            .filter(acl -> RegexUtils.isResourceCoveredByRegex(acl.getMetadata().getName(), nameFilterPatterns))
            .toList();
    }

    /**
     * Find all ACLs that a given namespace granted to other namespaces, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllRelatedToNamespaceByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAllRelatedToNamespace(namespace)
            .stream()
            .filter(acl -> RegexUtils.isResourceCoveredByRegex(acl.getMetadata().getName(), nameFilterPatterns))
            .toList();
    }

    /**
     * Find all owner-ACLs on a resource granted to a given namespace.
     *
     * @param namespace    The namespace
     * @param resourceType The resource
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findResourceOwnerGrantedToNamespace(Namespace namespace,
                                                                        AccessControlEntry.ResourceType resourceType) {
        return findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(acl -> acl.getSpec().getGrantedTo().equals(namespace.getMetadata().getName())
                && acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER
                && acl.getSpec().getResourceType() == resourceType)
            .toList();
    }

    /**
     * Find all public granted ACLs.
     *
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllPublicGrantedTo() {
        return accessControlEntryRepository.findAll()
            .stream()
            .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO))
            .toList();
    }

    /**
     * Find all ACLs of given namespace.
     *
     * @param namespace The namespace
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllForNamespace(Namespace namespace) {
        return findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(accessControlEntry -> accessControlEntry.getMetadata().getNamespace()
                .equals(namespace.getMetadata().getName()))
            .toList();
    }

    /**
     * Find all ACLs of given cluster.
     *
     * @param cluster The cluster
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllForCluster(String cluster) {
        return accessControlEntryRepository.findAll()
            .stream()
            .filter(accessControlEntry -> accessControlEntry.getMetadata().getCluster().equals(cluster))
            .toList();
    }

    /**
     * Find all the ACLs on all clusters.
     *
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAll() {
        return new ArrayList<>(accessControlEntryRepository.findAll());
    }

    /**
     * Is namespace owner of the given resource.
     *
     * @param namespace    The namespace
     * @param resourceType The resource type to filter
     * @param resource     The resource name
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfResource(String namespace, AccessControlEntry.ResourceType resourceType,
                                              String resource) {
        return accessControlEntryRepository.findAll()
            .stream()
            .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace))
            .filter(accessControlEntry -> accessControlEntry.getSpec().getPermission()
                == AccessControlEntry.Permission.OWNER)
            .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == resourceType)
            .anyMatch(accessControlEntry -> switch (accessControlEntry.getSpec().getResourcePatternType()) {
                case PREFIXED -> resource.startsWith(accessControlEntry.getSpec().getResource());
                case LITERAL -> resource.equals(accessControlEntry.getSpec().getResource());
            });
    }

    /**
     * Find an ACL by name.
     *
     * @param namespace The namespace
     * @param name      The ACL name
     * @return An optional ACL
     */
    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return accessControlEntryRepository.findByName(namespace, name);
    }

    /**
     * Check if the given resource is covered by any given ACLs.
     *
     * @param acls         The OWNER ACL list on resource
     * @param resourceName The resource name to check ACL against
     * @return true if there is any OWNER ACL concerning the given resource, false otherwise
     */
    public boolean isResourceCoveredByAcls(List<AccessControlEntry> acls, String resourceName) {
        return acls
            .stream()
            .anyMatch(acl -> switch (acl.getSpec().getResourcePatternType()) {
                case PREFIXED -> resourceName.startsWith(acl.getSpec().getResource());
                case LITERAL -> resourceName.equals(acl.getSpec().getResource());
            });
    }
}