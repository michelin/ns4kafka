package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Status.StatusCauses;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import io.micronaut.context.ApplicationContext;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class AccessControlEntryService {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AccessControlEntryService.class);
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;
    @Inject
    ApplicationContext applicationContext;

    public List<StatusCauses> validate(AccessControlEntry accessControlEntry, Namespace namespace) {
        List<StatusCauses> validationErrors = new ArrayList<>();
        // Which resource can be granted cross namespaces ? TOPIC
        List<AccessControlEntry.ResourceType> allowedResourceTypes =
                List.of(AccessControlEntry.ResourceType.TOPIC);
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
            validationErrors.add(StatusCauses.builder()
                    .field("resourceType")
                    .reason("Invalid")
                    .message("Value must be one of [" + allowedResourceTypes.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]")
                    .build());
        }
        if (!allowedPermissions.contains(accessControlEntry.getSpec().getPermission())) {

            validationErrors.add(StatusCauses.builder()
                    .field("permission")
                    .reason("Invalid")
                    .message("Value must be one of [" + allowedPermissions.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]")
                    .build());
        }
        if (!allowedPatternTypes.contains(accessControlEntry.getSpec().getResourcePatternType())) {
            validationErrors.add(StatusCauses.builder()
                    .field("patternType")
                    .reason("Invalid")
                    .message("Value must be one of [" + allowedPatternTypes.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]")
                    .build());
        }


        // GrantedTo Namespace exists ?
        NamespaceService namespaceService = applicationContext.getBean(NamespaceService.class);
        Optional<Namespace> grantedToNamespace = namespaceService.findByName(accessControlEntry.getSpec().getGrantedTo());
        if (grantedToNamespace.isEmpty()) {
            validationErrors.add(StatusCauses.builder()
                    .field("grantedTo")
                    .reason("Invalid")
                    .message("Namespace doesn't exist")
                    .build());
        }

        // Are you dumb ?
        if (namespace.getMetadata().getName().equals(accessControlEntry.getSpec().getGrantedTo())) {
            validationErrors.add(StatusCauses.builder()
                    .field("grantedTo")
                    .reason("Invalid")
                    .message("Why would you grant to yourself ?!")
                    .build());
        }

        if (!isOwnerOfTopLevelAcl(accessControlEntry, namespace)) {
            validationErrors.add(StatusCauses.builder()
                    .field("ressourcePatternType")
                    .reason("Invalid")
                    .message("Namespace is neither OWNER of LITERAL:resource nor top-level PREFIXED:resource")
                    .build());
        }
        return validationErrors;
    }

    public List<StatusCauses> validateAsAdmin(AccessControlEntry accessControlEntry, Namespace namespace) {
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
                .map(ace -> StatusCauses.builder()
                    .field("resource")
                    .reason("Invalid")
                    .message(String.format("AccessControlEntry overlaps with existing one: %s", ace))
                    .build())
                .collect(Collectors.toList());
    }

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
                    if (ace.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL &&
                            accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.LITERAL &&
                            accessControlEntry.getSpec().getResource().equals(ace.getSpec().getResource())) {
                        return true;
                    }
                    return false;
                });
    }

    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        return accessControlEntryRepository.create(accessControlEntry);
    }

    public void delete(AccessControlEntry accessControlEntry) {
        accessControlEntryRepository.delete(accessControlEntry);
    }

    public List<AccessControlEntry> findAllGrantedToNamespace(Namespace namespace) {
        return accessControlEntryRepository.findAll().stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace.getMetadata().getName()))
                .collect(Collectors.toList());
    }

    public List<AccessControlEntry> findAllForNamespace(Namespace namespace) {
        return accessControlEntryRepository.findAll().stream()
                .filter(accessControlEntry -> accessControlEntry.getMetadata().getNamespace().equals(namespace.getMetadata().getName()))
                .collect(Collectors.toList());
    }

    public List<AccessControlEntry> findAllForCluster(String cluster) {
        return accessControlEntryRepository.findAll().stream()
                .filter(accessControlEntry -> accessControlEntry.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

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

    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return accessControlEntryRepository.findByName(namespace, name);
    }
}
