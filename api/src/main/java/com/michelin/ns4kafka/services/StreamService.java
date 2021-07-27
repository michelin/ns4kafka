package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.StreamRepository;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Singleton
public class StreamService {

    @Inject
    StreamRepository streamRepository;

    @Inject
    AccessControlEntryService accessControlEntryService;

    public List<KafkaStream> findAllForNamespace(Namespace namespace) {
        //TODO to fix
        return streamRepository.findAllForCluster(namespace.getMetadata().getCluster());

    }

    public Optional<KafkaStream> findByName(Namespace namespace, String stream) {
        //TODO
        return null;
    }

    public boolean isNamespaceOwnerOfStream(String namespace, String stream) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.TOPIC, stream);
    }
}
