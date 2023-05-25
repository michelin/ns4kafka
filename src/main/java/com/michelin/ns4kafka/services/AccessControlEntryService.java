package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.services.executors.AccessControlEntryAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class AccessControlEntryService {
    public static final String PUBLIC_GRANTED_TO = "*";

    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    @Inject
    ApplicationContext applicationContext;

    /**
     * Validate a new ACL
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
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getResourceType() +
                    " for resourceType: Value must be one of [" +
                    allowedResourceTypes.stream().map(Object::toString).collect(Collectors.joining(", ")) +
                    "]");
        }

        if (!allowedPermissions.contains(accessControlEntry.getSpec().getPermission())) {
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getPermission() +
                    " for permission: Value must be one of [" +
                    allowedPermissions.stream().map(Object::toString).collect(Collectors.joining(", ")) +
                    "]");
        }

        if (!allowedPatternTypes.contains(accessControlEntry.getSpec().getResourcePatternType())) {
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getResourcePatternType() +
                    " for patternType: Value must be one of [" +
                    allowedPatternTypes.stream().map(Object::toString).collect(Collectors.joining(", ")) +
                    "]");
        }

        // GrantedTo Namespace exists ?
        NamespaceService namespaceService = applicationContext.getBean(NamespaceService.class);
        Optional<Namespace> grantedToNamespace = namespaceService.findByName(accessControlEntry.getSpec().getGrantedTo());
        if (grantedToNamespace.isEmpty() && !accessControlEntry.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO)) {
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getGrantedTo() + " for grantedTo: Namespace doesn't exist");
        }

        // Are you dumb ?
        if (namespace.getMetadata().getName().equals(accessControlEntry.getSpec().getGrantedTo())) {
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getGrantedTo() + " for grantedTo: Why would you grant to yourself ?!");
        }

        if (!isOwnerOfTopLevelAcl(accessControlEntry, namespace)) {
            validationErrors.add("Invalid grant " + accessControlEntry.getSpec().getResourcePatternType() + ":" +
                    accessControlEntry.getSpec().getResource() +
                    " : Namespace is neither OWNER of LITERAL: resource nor top-level PREFIXED:resource");
        }

        return validationErrors;
    }

    /**
     * Validate a new ACL created by an admin
     *
     * @param accessControlEntry The ACL
     * @param namespace          The namespace
     * @return A list of validation errors
     */
    public List<String> validateAsAdmin(AccessControlEntry accessControlEntry, Namespace namespace) {
        // another namespace is already OWNER of PREFIXED or LITERAL resource
        // exemple :
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
                // don't include the ACL if it's itself (namespace+name)
                .filter(ace -> !ace.getMetadata().getNamespace().equals(namespace.getMetadata().getName()) ||
                        !ace.getMetadata().getName().equals(accessControlEntry.getMetadata().getName()))
                .filter(ace -> ace.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(ace -> ace.getSpec().getResourceType() == accessControlEntry.getSpec().getResourceType())
                .filter(ace -> {
                    // new PREFIXED ACL would cover existing ACLs
                    boolean parentOverlap = false;
                    if (accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED) {
                        parentOverlap = ace.getSpec().getResource().startsWith(accessControlEntry.getSpec().getResource());
                    }
                    // new ACL would be covered by a PREFIXED existing ACLs
                    boolean childOverlap = false;
                    if (ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED) {
                        childOverlap = accessControlEntry.getSpec().getResource().startsWith(ace.getSpec().getResource());
                    }

                    boolean same = accessControlEntry.getSpec().getResource().equals(ace.getSpec().getResource());

                    return same || parentOverlap || childOverlap;

                })
                .map(ace -> String.format("AccessControlEntry overlaps with existing one: %s", ace))
                .toList();
    }

    /**
     * Is namespace owner of given ACL
     *
     * @param accessControlEntry The ACL
     * @param namespace          The namespace
     * @return true if it is, false otherwise
     */
    public boolean isOwnerOfTopLevelAcl(AccessControlEntry accessControlEntry, Namespace namespace) {
        // Grantor Namespace is OWNER of Resource + ResourcePattern ?
        return findAllGrantedToNamespace(namespace).stream()
                .filter(ace -> ace.getSpec().getResourceType() == accessControlEntry.getSpec().getResourceType() &&
                        ace.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .anyMatch(ace -> {
                    // if grantor is owner of PREFIXED resource that starts with
                    // owner  PREFIXED: priv_bsm_
                    // grants LITERAL : priv_bsm_topic  OK
                    // grants PREFIXED: priv_bsm_topic  OK
                    // grants PREFIXED: priv_b          NO
                    // grants LITERAL : priv_b          NO
                    // grants PREFIXED: priv_bsm_       OK
                    // grants LITERAL : pric_bsm_       OK
                    if (ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED &&
                            accessControlEntry.getSpec().getResource().startsWith(ace.getSpec().getResource())) {
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
                    return ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL &&
                            accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL &&
                            accessControlEntry.getSpec().getResource().equals(ace.getSpec().getResource());
                });
    }

    /**
     * Create an ACL in internal topic
     *
     * @param accessControlEntry The ACL
     * @return The created ACL
     */
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        return accessControlEntryRepository.create(accessControlEntry);
    }

    /**
     * Delete an ACL from broker and from internal topic
     *
     * @param namespace The namespace
     * @param accessControlEntry The ACL
     */
    public void delete(Namespace namespace, AccessControlEntry accessControlEntry) {
        AccessControlEntryAsyncExecutor accessControlEntryAsyncExecutor = applicationContext.getBean(AccessControlEntryAsyncExecutor.class,
                Qualifiers.byName(accessControlEntry.getMetadata().getCluster()));
        accessControlEntryAsyncExecutor.deleteNs4KafkaACL(namespace, accessControlEntry);

        accessControlEntryRepository.delete(accessControlEntry);
    }

    /**
     * Find all ACLs granted to given namespace
     * Will also return public granted ACLs
     *
     * @param namespace The namespace
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllGrantedToNamespace(Namespace namespace) {
        return accessControlEntryRepository.findAll()
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace.getMetadata().getName()) ||
                                accessControlEntry.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO))
                .toList();
    }

    /**
     * Find all public granted ACLs
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
     * Find all ACLs of given namespace
     *
     * @param namespace The namespace
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllForNamespace(Namespace namespace) {
        return accessControlEntryRepository.findAll().stream()
                .filter(accessControlEntry -> accessControlEntry.getMetadata().getNamespace().equals(namespace.getMetadata().getName()))
                .toList();
    }

    /**
     * Find all ACLs of given cluster
     *
     * @param cluster The cluster
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAllForCluster(String cluster) {
        return accessControlEntryRepository.findAll().stream()
                .filter(accessControlEntry -> accessControlEntry.getMetadata().getCluster().equals(cluster))
                .toList();
    }

    /**
     * Find all the ACLs on all clusters
     * @return A list of ACLs
     */
    public List<AccessControlEntry> findAll() {
        return new ArrayList<>(accessControlEntryRepository.findAll());
    }

    /**
     * Does given namespace is owner of the given resource ?
     *
     * @param namespace    The namespace
     * @param resourceType The resource type to filter
     * @param resource     The resource name
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfResource(String namespace, AccessControlEntry.ResourceType resourceType, String resource) {
        return accessControlEntryRepository.findAll()
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                .filter(accessControlEntry -> accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == resourceType)
                .anyMatch(accessControlEntry -> {
                    switch (accessControlEntry.getSpec().getResourcePatternType()) {
                        case PREFIXED:
                            return resource.startsWith(accessControlEntry.getSpec().getResource());
                        case LITERAL:
                            return resource.equals(accessControlEntry.getSpec().getResource());
                    }
                    return false;
                });
    }

    /**
     * Find an ACL by name
     *
     * @param namespace The namespace
     * @param name      The ACL name
     * @return An optional ACL
     */
    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return accessControlEntryRepository.findByName(namespace, name);
    }
}
