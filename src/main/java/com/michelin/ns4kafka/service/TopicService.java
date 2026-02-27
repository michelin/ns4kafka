/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidImmutableValue;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTopicCleanUpPolicy;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;

/** Service to manage topics. */
@Singleton
public class TopicService {
    @Inject
    private TopicRepository topicRepository;

    @Inject
    private AclService aclService;

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private List<ManagedClusterProperties> managedClusterProperties;

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
        List<AccessControlEntry> acls =
                aclService.findResourceOwnerGrantedToNamespace(namespace, AccessControlEntry.ResourceType.TOPIC);
        return topicRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
                .filter(topic -> aclService.isResourceCoveredByAcls(
                        acls, topic.getMetadata().getName()))
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
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAllForNamespace(namespace).stream()
                .filter(topic ->
                        RegexUtils.isResourceCoveredByRegex(topic.getMetadata().getName(), nameFilterPatterns))
                .toList();
    }

    /**
     * Find a topic by namespace and name.
     *
     * @param namespace The namespace
     * @param topic The topic name
     * @return An optional topic
     */
    public Optional<Topic> findByName(Namespace namespace, String topic) {
        return findAllForNamespace(namespace).stream()
                .filter(t -> t.getMetadata().getName().equals(topic))
                .findFirst();
    }

    /**
     * Is given namespace owner of the given topic.
     *
     * @param namespace The namespace
     * @param topic The topic
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfTopic(String namespace, String topic) {
        return aclService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.TOPIC, topic);
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
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(
                TopicAsyncExecutor.class, Qualifiers.byName(topic.getMetadata().getCluster()));
        topicAsyncExecutor.deleteTopics(List.of(topic));

        topicRepository.delete(topic);
    }

    /**
     * Delete multiple topics.
     *
     * @param topics The topics list
     */
    public void deleteTopics(List<Topic> topics) throws InterruptedException, ExecutionException, TimeoutException {
        if (topics == null || topics.isEmpty()) {
            return;
        }

        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(
                TopicAsyncExecutor.class,
                Qualifiers.byName(topics.getFirst().getMetadata().getCluster()));
        topicAsyncExecutor.deleteTopics(topics);

        topics.forEach(topic -> topicRepository.delete(topic));
    }

    /**
     * List all topics colliding with existing topics on broker but not in Ns4Kafka.
     *
     * @param namespace The namespace
     * @param topic The topic
     * @return The list of colliding topics
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException Any timeout exception
     */
    public List<String> findCollidingTopics(Namespace namespace, Topic topic)
            throws InterruptedException, ExecutionException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(
                TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        try {
            List<String> clusterTopics = topicAsyncExecutor.listBrokerTopicNames();
            return clusterTopics.stream()
                    // existing topics with the exact same name (and not currently in Ns4Kafka) should not interfere
                    // this topic could be created on Ns4Kafka during "import" step
                    .filter(clusterTopic -> !topic.getMetadata().getName().equals(clusterTopic))
                    .filter(clusterTopic ->
                            hasCollision(clusterTopic, topic.getMetadata().getName()))
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
     * @param newTopic The new topic
     * @return A list of validation errors
     */
    public List<String> validateTopicUpdate(Namespace namespace, Topic existingTopic, Topic newTopic) {
        List<String> validationErrors = new ArrayList<>();

        if (existingTopic.getSpec().getPartitions() != newTopic.getSpec().getPartitions()) {
            validationErrors.add(invalidImmutableValue(
                    "partitions", String.valueOf(newTopic.getSpec().getPartitions())));
        }

        if (existingTopic.getSpec().getReplicationFactor() != newTopic.getSpec().getReplicationFactor()) {
            validationErrors.add(invalidImmutableValue(
                    "replication.factor", String.valueOf(newTopic.getSpec().getReplicationFactor())));
        }

        Optional<ManagedClusterProperties> topicCluster = managedClusterProperties.stream()
                .filter(cluster -> namespace.getMetadata().getCluster().equals(cluster.getName()))
                .findFirst();

        boolean isConfluentCloud =
                topicCluster.isPresent() && topicCluster.get().isConfluentCloud();

        String existingCleanUpPolicy = Optional.ofNullable(
                        existingTopic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG))
                .orElse(CLEANUP_POLICY_DELETE);
        String newCleanUpPolicy = Optional.ofNullable(
                        newTopic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG))
                .orElse(CLEANUP_POLICY_DELETE);

        if (isConfluentCloud
                && existingCleanUpPolicy.equals(CLEANUP_POLICY_DELETE)
                && (newCleanUpPolicy.equals(CLEANUP_POLICY_DELETE + "," + CLEANUP_POLICY_COMPACT)
                        || newCleanUpPolicy.equals(CLEANUP_POLICY_COMPACT + "," + CLEANUP_POLICY_DELETE))) {
            validationErrors.add(invalidTopicCleanUpPolicy(newCleanUpPolicy));
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
     * List all topics of a given namespace that are not synchronized to Ns4Kafka, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @return The list of topics
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException Any timeout exception
     */
    public List<Topic> listUnsynchronizedTopicsByWildcardName(Namespace namespace, String name)
            throws ExecutionException, InterruptedException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(
                TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        List<AccessControlEntry> acls =
                aclService.findResourceOwnerGrantedToNamespace(namespace, AccessControlEntry.ResourceType.TOPIC);

        Set<String> ns4KafkaTopicNames =
                topicRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
                        .map(topic -> topic.getMetadata().getName())
                        .collect(Collectors.toSet());

        return topicAsyncExecutor
                .collectBrokerTopicsFromNames(topicAsyncExecutor.listBrokerTopicNames().stream()
                        .filter(topic -> !ns4KafkaTopicNames.contains(topic)
                                && aclService.isResourceCoveredByAcls(acls, topic)
                                && RegexUtils.isResourceCoveredByRegex(topic, nameFilterPatterns))
                        .toList())
                .values()
                .stream()
                .toList();
    }

    /**
     * Import topics from broker to Ns4Kafka storage.
     *
     * @param namespace The namespace
     * @param topics The list of topics to import
     */
    public void importTopics(Namespace namespace, List<Topic> topics) {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(
                TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        topicAsyncExecutor.importTopics(topics);
    }

    /**
     * Validate if a topic can be eligible for records deletion.
     *
     * @param topic The topic to delete records
     * @return A list of errors
     */
    public List<String> validateDeleteRecordsTopic(Topic topic) {
        List<String> errors = new ArrayList<>();

        String cleanUpPolicy = Optional.ofNullable(topic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG))
                .orElse(CLEANUP_POLICY_DELETE);

        if (cleanUpPolicy.equals(CLEANUP_POLICY_COMPACT)) {
            errors.add(invalidTopicDeleteRecords());
        }

        return errors;
    }

    /**
     * For a given topic, get each latest offset by partition in order to delete all the records before these offsets.
     *
     * @param topic The topic to delete records
     * @return A map of offsets by topic-partitions
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, Long> prepareRecordsToDelete(Topic topic)
            throws ExecutionException, InterruptedException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(
                TopicAsyncExecutor.class, Qualifiers.byName(topic.getMetadata().getCluster()));

        try {
            return topicAsyncExecutor.prepareRecordsToDelete(topic.getMetadata().getName()).entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey, kv -> kv.getValue().beforeOffset()));
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
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(
                TopicAsyncExecutor.class, Qualifiers.byName(topic.getMetadata().getCluster()));

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
        return topic.getSpec().getTags().stream().allMatch(tag -> tag.matches("^[a-zA-Z]\\w*$"));
    }

    /**
     * Validate tags for topic.
     *
     * @param namespace The namespace
     * @param topic The topic which contains tags
     * @return A list of validation errors
     */
    public List<String> validateTags(Namespace namespace, Topic topic) {
        List<String> validationErrors = new ArrayList<>();

        Optional<ManagedClusterProperties> topicCluster = managedClusterProperties.stream()
                .filter(cluster -> namespace.getMetadata().getCluster().equals(cluster.getName()))
                .findFirst();

        if (topicCluster.isPresent() && !topicCluster.get().isConfluentCloud()) {
            validationErrors.add(
                    invalidTopicTags(String.join(",", topic.getSpec().getTags())));
            return validationErrors;
        }

        if (!isTagsFormatValid(topic)) {
            validationErrors.add(
                    invalidTopicTagsFormat(String.join(",", topic.getSpec().getTags())));
            return validationErrors;
        }

        return validationErrors;
    }
}
