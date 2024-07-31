package com.michelin.ns4kafka.service;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.repository.StreamRepository;
import com.michelin.ns4kafka.service.executor.AccessControlEntryAsyncExecutor;
import com.michelin.ns4kafka.util.RegexUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * Service to manage Kafka Streams.
 */
@Singleton
public class StreamService {
    @Inject
    StreamRepository streamRepository;

    @Inject
    AccessControlEntryService accessControlEntryService;

    @Inject
    ApplicationContext applicationContext;

    /**
     * Find all Kafka Streams of a given namespace.
     *
     * @param namespace The namespace
     * @return A list of Kafka Streams
     */
    public List<KafkaStream> findAllForNamespace(Namespace namespace) {
        return streamRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
            .filter(stream -> stream.getMetadata().getNamespace().equals(namespace.getMetadata().getName()))
            .toList();
    }

    /**
     * Find all Kafka Streams of a given namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name filter
     * @return A list of Kafka Streams
     */
    public List<KafkaStream> findByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.wildcardStringsToRegexPatterns(List.of(name));
        return findAllForNamespace(namespace)
            .stream()
            .filter(stream -> RegexUtils.filterByPattern(stream.getMetadata().getName(), nameFilterPatterns))
            .toList();
    }

    /**
     * Find a Kafka Streams by namespace and name.
     *
     * @param namespace The namespace
     * @param stream    The Kafka Streams name
     * @return An optional Kafka Streams
     */
    public Optional<KafkaStream> findByName(Namespace namespace, String stream) {
        return findAllForNamespace(namespace).stream()
            .filter(kafkaStream -> kafkaStream.getMetadata().getName().equals(stream))
            .findFirst();
    }

    /**
     * Is given namespace owner of the given Kafka Streams.
     * Kafka Streams ownership is determined by both topic and group ownership on prefixed resource.
     * This is because Kafka Streams "application.id" is a consumer group but also a prefix for internal topic names.
     *
     * @param namespace The namespace
     * @param resource  The Kafka Streams
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfKafkaStream(Namespace namespace, String resource) {
        return new HashSet<>(accessControlEntryService.findAllGrantedToNamespace(namespace)
            .stream()
            .filter(accessControlEntry -> accessControlEntry.getSpec().getPermission()
                == AccessControlEntry.Permission.OWNER)
            .filter(accessControlEntry -> accessControlEntry.getSpec().getResourcePatternType()
                == AccessControlEntry.ResourcePatternType.PREFIXED)
            .filter(accessControlEntry -> resource.startsWith(accessControlEntry.getSpec().getResource()))
            .map(accessControlEntry -> accessControlEntry.getSpec().getResourceType())
            .toList())
            .containsAll(List.of(AccessControlEntry.ResourceType.TOPIC, AccessControlEntry.ResourceType.GROUP));
    }

    /**
     * Create a given Kafka Stream.
     *
     * @param stream The Kafka Stream to create
     * @return The created Kafka Stream
     */
    public KafkaStream create(KafkaStream stream) {
        return streamRepository.create(stream);
    }

    /**
     * Delete a given Kafka Stream.
     *
     * @param stream The Kafka Stream
     */
    public void delete(Namespace namespace, KafkaStream stream) {
        AccessControlEntryAsyncExecutor accessControlEntryAsyncExecutor =
            applicationContext.getBean(AccessControlEntryAsyncExecutor.class,
                Qualifiers.byName(stream.getMetadata().getCluster()));
        accessControlEntryAsyncExecutor.deleteKafkaStreams(namespace, stream);

        streamRepository.delete(stream);
    }
}
