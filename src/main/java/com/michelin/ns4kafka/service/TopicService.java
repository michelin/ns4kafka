package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidImmutableValue;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTopicCleanupPolicy;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTopicDeleteRecords;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTopicTags;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTopicTagsFormat;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TopicRepository;
import com.michelin.ns4kafka.service.executor.TopicAsyncExecutor;
import com.michelin.ns4kafka.util.RegexUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;

/**
 * Service to manage topics.
 */
@Singleton
public class TopicService {
    @Inject
    TopicRepository topicRepository;

    @Inject
    AclService aclService;

    @Inject
    ApplicationContext applicationContext;

    @Inject
    List<ManagedClusterProperties> managedClusterProperties;

    /**
     * Find all topics.
     *
     * @return The list of topics
     */
    public List<Topic> findAll() {
        return topicRepository.findAll();
    }

    /**
     * Find all topics by given namespace.
     *
     * @param namespace The namespace
     * @return A list of topics
     */
    public List<Topic> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = aclService
            .findResourceOwnerGrantedToNamespace(namespace, AccessControlEntry.ResourceType.TOPIC);
        return topicRepository.findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(topic -> aclService.isAnyAclOfResource(acls, topic.getMetadata().getName()))
            .toList();
    }

    /**
     * Find all topics of a given namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name filter
     * @return A list of topics
     */
    public List<Topic> findByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.wildcardStringsToRegexPatterns(List.of(name));
        return findAllForNamespace(namespace)
            .stream()
            .filter(topic -> RegexUtils.filterByPattern(topic.getMetadata().getName(), nameFilterPatterns))
            .toList();
    }

    /**
     * Find a topic by namespace and name.
     *
     * @param namespace The namespace
     * @param topic     The topic name
     * @return An optional topic
     */
    public Optional<Topic> findByName(Namespace namespace, String topic) {
        return findAllForNamespace(namespace)
            .stream()
            .filter(t -> t.getMetadata().getName().equals(topic))
            .findFirst();
    }

    /**
     * Is given namespace owner of the given topic.
     *
     * @param namespace The namespace
     * @param topic     The topic
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfTopic(String namespace, String topic) {
        return aclService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.TOPIC,
            topic);
    }

    /**
     * Create a given topic.
     *
     * @param topic The topic to create
     * @return The created topic
     */
    public Topic create(Topic topic) {
        return topicRepository.create(topic);
    }

    /**
     * Delete a given topic.
     *
     * @param topic The topic
     */
    public void delete(Topic topic) throws InterruptedException, ExecutionException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(topic.getMetadata().getCluster()));
        topicAsyncExecutor.deleteTopic(topic);

        topicRepository.delete(topic);
    }

    /**
     * List all topics colliding with existing topics on broker but not in Ns4Kafka.
     *
     * @param namespace The namespace
     * @param topic     The topic
     * @return The list of colliding topics
     * @throws ExecutionException   Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException     Any timeout exception
     */
    public List<String> findCollidingTopics(Namespace namespace, Topic topic)
        throws InterruptedException, ExecutionException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(namespace.getMetadata().getCluster()));

        try {
            List<String> clusterTopics = topicAsyncExecutor.listBrokerTopicNames();
            return clusterTopics.stream()
                // existing topics with the exact same name (and not currently in Ns4Kafka) should not interfere
                // this topic could be created on Ns4Kafka during "import" step
                .filter(clusterTopic -> !topic.getMetadata().getName().equals(clusterTopic))
                .filter(clusterTopic -> hasCollision(clusterTopic, topic.getMetadata().getName()))
                .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException(e.getMessage());
        }
    }

    /**
     * Validate existing topic can be updated with new given configs.
     *
     * @param existingTopic The existing topic
     * @param newTopic      The new topic
     * @return A list of validation errors
     */
    public List<String> validateTopicUpdate(Namespace namespace, Topic existingTopic, Topic newTopic) {
        List<String> validationErrors = new ArrayList<>();

        if (existingTopic.getSpec().getPartitions() != newTopic.getSpec().getPartitions()) {
            validationErrors.add(invalidImmutableValue("partitions",
                String.valueOf(newTopic.getSpec().getPartitions())));
        }

        if (existingTopic.getSpec().getReplicationFactor() != newTopic.getSpec().getReplicationFactor()) {
            validationErrors.add(invalidImmutableValue("replication.factor",
                String.valueOf(newTopic.getSpec().getReplicationFactor())));
        }

        Optional<ManagedClusterProperties> topicCluster = managedClusterProperties
            .stream()
            .filter(cluster -> namespace.getMetadata().getCluster().equals(cluster.getName()))
            .findFirst();

        boolean isConfluentCloud = topicCluster.isPresent() && topicCluster.get().isConfluentCloud();

        if (isConfluentCloud
            && existingTopic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG).equals(CLEANUP_POLICY_DELETE)
            && newTopic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG).equals(CLEANUP_POLICY_COMPACT)) {
            validationErrors.add(invalidTopicCleanupPolicy(newTopic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG)));
        }

        return validationErrors;
    }

    /**
     * Check if topics collide with "_" instead of ".".
     *
     * @param topicA The first topic
     * @param topicB The second topic
     * @return true if it does, false otherwise
     */
    private boolean hasCollision(String topicA, String topicB) {
        return topicA.replace('.', '_').equals(topicB.replace('.', '_'));
    }

    /**
     * List the topics that are not synchronized to Ns4Kafka by namespace.
     *
     * @param namespace The namespace
     * @return The list of topics
     * @throws ExecutionException   Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException     Any timeout exception
     */
    public List<Topic> listUnsynchronizedTopics(Namespace namespace)
        throws ExecutionException, InterruptedException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(namespace.getMetadata().getCluster()));

        // List topics for this namespace
        List<String> topicNames = listUnsynchronizedTopicNames(namespace);

        // Get topics definitions
        Collection<Topic> unsynchronizedTopics = topicAsyncExecutor.collectBrokerTopicsFromNames(topicNames)
            .values();

        return new ArrayList<>(unsynchronizedTopics);
    }

    /**
     * List the topic names that are not synchronized to ns4kafka by namespace.
     *
     * @param namespace The namespace
     * @return The list of topic names
     * @throws ExecutionException   Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException     Any timeout exception
     */
    public List<String> listUnsynchronizedTopicNames(Namespace namespace)
        throws ExecutionException, InterruptedException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(namespace.getMetadata().getCluster()));

        return topicAsyncExecutor.listBrokerTopicNames()
            .stream()
            // ...that belongs to this namespace
            .filter(topic -> isNamespaceOwnerOfTopic(namespace.getMetadata().getName(), topic))
            // ...and aren't in ns4kafka storage
            .filter(topic -> findByName(namespace, topic).isEmpty())
            .toList();
    }

    /**
     * Validate if a topic can be eligible for records deletion.
     *
     * @param deleteRecordsTopic The topic to delete records
     * @return A list of errors
     */
    public List<String> validateDeleteRecordsTopic(Topic deleteRecordsTopic) {
        List<String> errors = new ArrayList<>();

        if (deleteRecordsTopic.getSpec().getConfigs().get("cleanup.policy").equals("compact")) {
            errors.add(invalidTopicDeleteRecords());
        }

        return errors;
    }

    /**
     * For a given topic, get each latest offset by partition in order to delete all the records
     * before these offsets.
     *
     * @param topic The topic to delete records
     * @return A map of offsets by topic-partitions
     * @throws ExecutionException   Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, Long> prepareRecordsToDelete(Topic topic)
        throws ExecutionException, InterruptedException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(topic.getMetadata().getCluster()));

        try {
            return topicAsyncExecutor.prepareRecordsToDelete(topic.getMetadata().getName())
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, kv -> kv.getValue().beforeOffset()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException(e.getMessage());
        }
    }

    /**
     * Delete the records for each partition, before each offset.
     *
     * @param recordsToDelete The offsets by topic-partitions
     * @return The new offsets by topic-partitions
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, Long> deleteRecords(Topic topic, Map<TopicPartition, Long> recordsToDelete)
        throws InterruptedException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(topic.getMetadata().getCluster()));

        try {
            Map<TopicPartition, RecordsToDelete> recordsToDeleteMap = recordsToDelete.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, kv -> RecordsToDelete.beforeOffset(kv.getValue())));

            return topicAsyncExecutor.deleteRecords(recordsToDeleteMap);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException(e.getMessage());
        }
    }

    /**
     * Check if all topic tags respect confluent format (starts with letter followed by alphanumerical or underscore).
     *
     * @param topic The topic which contains tags
     * @return true if yes, false otherwise
     */
    public boolean isTagsFormatValid(Topic topic) {
        return topic.getSpec().getTags()
            .stream()
            .allMatch(tag -> tag.matches("^[a-zA-Z]\\w*$"));
    }

    /**
     * Validate tags for topic.
     *
     * @param namespace The namespace
     * @param topic     The topic which contains tags
     * @return A list of validation errors
     */
    public List<String> validateTags(Namespace namespace, Topic topic) {
        List<String> validationErrors = new ArrayList<>();

        Optional<ManagedClusterProperties> topicCluster = managedClusterProperties
            .stream()
            .filter(cluster -> namespace.getMetadata().getCluster().equals(cluster.getName()))
            .findFirst();

        if (topicCluster.isPresent() && !topicCluster.get().isConfluentCloud()) {
            validationErrors.add(invalidTopicTags(String.join(",", topic.getSpec().getTags())));
            return validationErrors;
        }

        if (!isTagsFormatValid(topic)) {
            validationErrors.add(invalidTopicTagsFormat(String.join(",", topic.getSpec().getTags())));
            return validationErrors;
        }

        return validationErrors;
    }
}
