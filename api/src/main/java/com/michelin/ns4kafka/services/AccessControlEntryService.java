package com.michelin.ns4kafka.services;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.controllers.AdminController.NamespaceCreationRequest;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;

import org.slf4j.LoggerFactory;

@Singleton
public class AccessControlEntryService {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AccessControlEntryService.class);

    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    public List<String> prefixInUse(NamespaceCreationRequest namespaceCreationRequest) {
        List<String> validationErrors = new ArrayList<>();
        List<AccessControlEntry> prefixInUse = accessControlEntryRepository
                .findAllForCluster(namespaceCreationRequest.getCluster()).stream()
                .filter(ace -> ace.getSpec()
                        .getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED)
                .filter(ace -> ace.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC)
                .filter(ace -> ace.getSpec().getResource().startsWith(namespaceCreationRequest.getPrefix())
                        || namespaceCreationRequest.getPrefix().startsWith(ace.getSpec().getResource()))
                .collect(Collectors.toList());
        if (!prefixInUse.isEmpty()) {
            validationErrors.add(String.format("Prefix overlaps with namespace %s: [%s]",
                    prefixInUse.get(0).getSpec().getGrantedTo(),
                    prefixInUse.get(0).getSpec().getResource()));
        }
        return validationErrors;
    }

    public boolean isNamespaceOwnerOfTopic(String namespace, String topic) {
        return accessControlEntryRepository.findAllGrantedToNamespace(namespace).stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec()
                        .getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(accessControlEntry -> accessControlEntry.getSpec()
                        .getResourceType() == AccessControlEntry.ResourceType.TOPIC)
                .anyMatch(accessControlEntry -> {
                    switch (accessControlEntry.getSpec().getResourcePatternType()) {
                    case PREFIXED:
                        return topic.startsWith(accessControlEntry.getSpec().getResource());
                    case LITERAL:
                        return topic.equals(accessControlEntry.getSpec().getResource());
                    }
                    return false;
                });
    }

    public List<AccessControlEntry> create(List<AccessControlEntry> accessControlEntryList) {
        accessControlEntryList.forEach(accessControlEntry -> {
            LOG.info(accessControlEntry.getSpec().toString());
            accessControlEntryRepository.create(accessControlEntry);

        });
        return accessControlEntryList;
    }

    public List<AccessControlEntry> deleteACLAttachedToNamespace(String namespace) {
        List<AccessControlEntry> accessControlEntryList = accessControlEntryRepository
                .findAllGrantedToNamespace(namespace);

        accessControlEntryList.forEach(accessControlEntry -> accessControlEntryRepository
                .deleteByName(accessControlEntry.getMetadata().getName()));

        return accessControlEntryList;
    }

    public void deleteByName(String acl){
        accessControlEntryRepository.deleteByName(acl);
    }

}
