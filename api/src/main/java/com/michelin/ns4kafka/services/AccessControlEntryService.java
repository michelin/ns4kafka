package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import io.micronaut.core.util.StringUtils;
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
    NamespaceService namespaceService;

    public List<String> validate(AccessControlEntry accessControlEntry, Namespace namespace) {
        List<String> validationErrors = new ArrayList<>();
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
        Optional<Namespace> grantedToNamespace = namespaceService.findByName(accessControlEntry.getSpec().getGrantedTo());
        if (grantedToNamespace.isEmpty()) {
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getGrantedTo() + " for grantedTo: Namespace doesn't exist");
        }

        // Are you dumb ?
        if (namespace.getMetadata().getName().equals(accessControlEntry.getSpec().getGrantedTo())) {
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getGrantedTo() + " for grantedTo: Why would you grant to yourself ?!");
        }

        if (!isOwnerOfTopLevelAcl(accessControlEntry, namespace)) {
            validationErrors.add("Invalid grant " + accessControlEntry.getSpec().getResourcePatternType() + ":" +
                    accessControlEntry.getSpec().getResource() +
                    " : Namespace is neither OWNER of LITERAL:resource nor top-level PREFIXED:resource");
        }
        return validationErrors;
    }

    public List<String> validateAsAdmin(AccessControlEntry accessControlEntry) {
        List<String> validationErrors = new ArrayList<>();
        if (StringUtils.isEmpty(accessControlEntry.getMetadata().getCluster())) {
            validationErrors.add("Invalid value null for cluster: Value must be non-null");
        }
        // GrantedTo Namespace exists ?
        Optional<Namespace> grantedToNamespace = namespaceService.findByName(accessControlEntry.getSpec().getGrantedTo());
        if (grantedToNamespace.isEmpty()) {
            validationErrors.add("Invalid value " + accessControlEntry.getSpec().getGrantedTo() + " for grantedTo: Namespace doesn't exist");
        }
        // ACE cluster is namespace cluster ?
        // this validation requires both validations above to be successful
        if (validationErrors.isEmpty() &&
                !accessControlEntry.getMetadata().getCluster().equals(grantedToNamespace.get().getMetadata().getCluster())) {
            validationErrors.add("Invalid value " + accessControlEntry.getMetadata().getCluster() + " for cluster: Value must be the same as the Namespace ["+grantedToNamespace.get().getMetadata().getCluster()+"]");
        }
        return validationErrors;
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

    public List<String> prefixInUse(String prefix, String cluster) {
        List<String> validationErrors = new ArrayList<>();
        List<AccessControlEntry> prefixInUse = findAllForCluster(cluster).stream()
                .filter(ace -> ace.getSpec()
                        .getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED)
                .filter(ace -> ace.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC)
                .filter(ace -> ace.getSpec().getResource().startsWith(prefix)
                        || prefix.startsWith(ace.getSpec().getResource()))
                .collect(Collectors.toList());
        if (!prefixInUse.isEmpty()) {
            validationErrors.add(String.format("Prefix overlaps with namespace %s: [%s]",
                    prefixInUse.get(0).getSpec().getGrantedTo(),
                    prefixInUse.get(0).getSpec().getResource()));
        }
        return validationErrors;
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
