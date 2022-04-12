package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.StreamRepository;
import com.michelin.ns4kafka.services.executors.AccessControlEntryAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class StreamService {
    /**
     * The Kafka Streams repository
     */
    @Inject
    StreamRepository streamRepository;

    /**
     * The ACL service
     */
    @Inject
    AccessControlEntryService accessControlEntryService;

    /**
     * The application context
     */
    @Inject
    ApplicationContext applicationContext;

    /**
     * Find all Kafka Streams by given namespace
     * @param namespace The namespace
     * @return A list of Kafka Streams
     */
    public List<KafkaStream> findAllForNamespace(Namespace namespace) {
        return streamRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
            .filter(stream -> stream.getMetadata().getNamespace().equals(namespace.getMetadata().getName()))
            .collect(Collectors.toList());
    }

    /**
     * Find a Kafka Streams by namespace and name
     * @param namespace The namespace
     * @param stream The Kafka Streams name
     * @return An optional Kafka Streams
     */
    public Optional<KafkaStream> findByName(Namespace namespace, String stream) {
        return findAllForNamespace(namespace).stream()
            .filter(kafkaStream -> kafkaStream.getMetadata().getName().equals(stream))
            .findFirst();
    }

    /**
     * Is given namespace owner of the given Kafka Streams
     * @param namespace The namespace
     * @param resource The Kafka Streams
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfKafkaStream(Namespace namespace, String resource) {
        // KafkaStream Ownership is determined by both Topic and Group ownership on PREFIXED resource,
        // this is because KafkaStream application.id is a consumer group but also a prefix for internal topic names
        return accessControlEntryService.findAllGrantedToNamespace(namespace)
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED)
                .filter(accessControlEntry -> resource.startsWith(accessControlEntry.getSpec().getResource()))
                .map(accessControlEntry -> accessControlEntry.getSpec().getResourceType())
                .collect(Collectors.toList())
                .containsAll(List.of(AccessControlEntry.ResourceType.TOPIC, AccessControlEntry.ResourceType.GROUP));
    }

    /**
     * Create a given Kafka Stream
     * @param stream The Kafka Stream to create
     * @return The created Kafka Stream
     */
    public KafkaStream create(KafkaStream stream) {
        return streamRepository.create(stream);
    }

    /**
     * Delete a given Kafka Stream
     * @param stream The Kafka Stream
     */
    public void delete(Namespace namespace, KafkaStream stream) {
        AccessControlEntryAsyncExecutor accessControlEntryAsyncExecutor = applicationContext.getBean(AccessControlEntryAsyncExecutor.class,
                Qualifiers.byName(stream.getMetadata().getCluster()));
        accessControlEntryAsyncExecutor.deleteKafkaStreams(namespace, stream);

        streamRepository.delete(stream);
    }
}
