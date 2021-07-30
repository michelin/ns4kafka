package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.StreamRepository;

import javax.inject.Inject;
import javax.inject.Singleton;
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

    public boolean isNamespaceOwnerOfStream(String namespace, String stream) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.TOPIC, stream);
    }

    public KafkaStream create(KafkaStream stream) {
        return streamRepository.create(stream);
    }

    public void delete(KafkaStream stream) {
        streamRepository.delete(stream);
    }
}
