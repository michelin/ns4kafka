package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class TopicService {
    @Inject
    TopicRepository topicRepository;
    @Inject
    AccessControlEntryService accessControlEntryService;

    public Optional<Topic> findByName(Namespace namespace, String topic) {
        return findAllForNamespace(namespace)
            .stream()
            .filter(t -> t.getMetadata().getName().equals(topic))
            .findFirst();
    }

    public List<Topic> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace);
        return topicRepository.findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(topic -> acls.stream().anyMatch(accessControlEntry -> {
                //no need to check accessControlEntry.Permission, we want READ, WRITE or OWNER
                if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC) {
                    switch (accessControlEntry.getSpec().getResourcePatternType()) {
                    case PREFIXED:
                        return topic.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                    case LITERAL:
                        return topic.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                    }
                }
                return false;
            }))
            .collect(Collectors.toList());
    }

    public boolean isNamespaceOwnerOfTopic(String namespace, String topic) {
        return accessControlEntryService.isNamespaceOwnerOfTopic(namespace, topic);
    }

    public Topic create(Topic topic) {
        return topicRepository.create(topic);
    }

    public void delete(Topic topic) {
        topicRepository.delete(topic);
    }
}
