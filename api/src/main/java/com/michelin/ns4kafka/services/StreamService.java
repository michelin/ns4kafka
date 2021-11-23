package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.StreamRepository;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class StreamService {

    @Inject
    StreamRepository streamRepository;

    @Inject
    AccessControlEntryService accessControlEntryService;

    public List<KafkaStream> findAllForNamespace(Namespace namespace) {
        return streamRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
            .filter(stream -> stream.getMetadata().getNamespace().equals(namespace.getMetadata().getName()))
            .collect(Collectors.toList());
    }

    public Optional<KafkaStream> findByName(Namespace namespace, String stream) {
        return findAllForNamespace(namespace).stream()
            .filter(kafkaStream -> kafkaStream.getMetadata().getName().equals(stream))
            .findFirst();
    }

    public boolean isNamespaceOwnerOfKafkaStream(Namespace namespace, String resource) {
        // KafkaStream Ownership is determined by both Topic and Group ownership on PREFIXED resource,
        // this is because KafkaStream application.id is a consumergroup but also a prefix for internal topic names
        return accessControlEntryService.findAllGrantedToNamespace(namespace)
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED)
                .filter(accessControlEntry -> resource.startsWith(accessControlEntry.getSpec().getResource()))
                .map(accessControlEntry -> accessControlEntry.getSpec().getResourceType())
                .collect(Collectors.toList())
                .containsAll(List.of(AccessControlEntry.ResourceType.TOPIC, AccessControlEntry.ResourceType.GROUP));
    }

    public KafkaStream create(KafkaStream stream) {
        return streamRepository.create(stream);
    }

    public void delete(KafkaStream stream) {
        streamRepository.delete(stream);
    }
}
