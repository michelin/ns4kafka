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
package com.michelin.ns4kafka.service.executor;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TopicRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.util.TopicConfigUtils;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

/** Topic executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class TopicAsyncExecutor {

    private final ManagedClusterProperties managedClusterProperties;
    private final TopicRepository topicRepository;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param topicRepository The topic repository
     */
    public TopicAsyncExecutor(ManagedClusterProperties managedClusterProperties, TopicRepository topicRepository) {
        this.managedClusterProperties = managedClusterProperties;
        this.topicRepository = topicRepository;
    }

    /** Run the topic synchronization. */
    public void run() {
        if (managedClusterProperties.isManageTopics()) {
            synchronizeTopics();
        }
    }

    /** Start the topic synchronization. */
    public void synchronizeTopics() {
        log.debug("Starting topic collection for cluster {}.", managedClusterProperties.getName());

        try {
            List<String> brokerTopicNames = listBrokerTopicNames();
            Map<String, Topic> brokerTopics =
                    brokerTopicNames.isEmpty() ? Map.of() : collectBrokerTopicsFromNames(brokerTopicNames);
            List<Topic> topics = topicRepository.findAllForCluster(managedClusterProperties.getName());

            List<Topic> toCreate = topics.stream()
                    .filter(topic ->
                            !brokerTopics.containsKey(topic.getMetadata().getName()))
                    .toList();

            List<Topic> toUpdate = topics.stream()
                    .filter(topic ->
                            brokerTopics.containsKey(topic.getMetadata().getName()))
                    .toList();

            if (!toCreate.isEmpty()) {
                log.atDebug()
                        .addArgument(() -> toCreate.stream()
                                .map(topic -> topic.getMetadata().getName())
                                .collect(Collectors.joining(",")))
                        .log("Topic(s) to create: {}.");

                createTopics(toCreate);
            }

            if (!toUpdate.isEmpty()) {
                Map<ConfigResource, Collection<AlterConfigOp>> configChanges = new HashMap<>();
                toUpdate.forEach(topic -> {
                    String topicName = topic.getMetadata().getName();
                    Collection<AlterConfigOp> changes = computeConfigChanges(
                            topic.getSpec().getConfigs(),
                            brokerTopics.get(topicName).getSpec().getConfigs());
                    if (!changes.isEmpty()) {
                        configChanges.put(new ConfigResource(ConfigResource.Type.TOPIC, topicName), changes);
                        return;
                    }

                    if (!topic.isSuccess() && isUnchangedSinceLastApply(topic)) {
                        // Configs already match the broker, only resolve the pending or failed status
                        topic.getMetadata().setGeneration(topic.getMetadata().getGeneration() + 1);
                        topic.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                        topicRepository.create(topic);
                    }
                });

                if (!configChanges.isEmpty()) {
                    log.atDebug()
                            .addArgument(() -> configChanges.keySet().stream()
                                    .map(ConfigResource::name)
                                    .collect(Collectors.joining(",")))
                            .log("Topic(s) to update: {}.");

                    alterTopics(configChanges, toUpdate);
                }
            }
        } catch (CancellationException | KafkaStoreException | ExecutionException | TimeoutException e) {
            log.error("An error occurred during the topic synchronization.", e);
        } catch (InterruptedException e) {
            log.error("An error occurred during the topic synchronization.", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * List all topic names on broker.
     *
     * @return All topic names
     */
    public List<String> listBrokerTopicNames() throws InterruptedException, ExecutionException, TimeoutException {
        return managedClusterProperties
                .getAdminClient()
                .listTopics()
                .listings()
                .get(managedClusterProperties.getTimeout().getTopic().getList(), TimeUnit.MILLISECONDS)
                .stream()
                .map(TopicListing::name)
                .toList();
    }

    /**
     * Collect all topics on broker from a list of topic names.
     *
     * @param topicNames The topic names
     * @return All topics by name
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    public Map<String, Topic> collectBrokerTopicsFromNames(List<String> topicNames)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, TopicDescription> topicDescriptions = managedClusterProperties
                .getAdminClient()
                .describeTopics(topicNames)
                .allTopicNames()
                .get();

        return managedClusterProperties
                .getAdminClient()
                .describeConfigs(topicNames.stream()
                        .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                        .toList())
                .all()
                .get(managedClusterProperties.getTimeout().getTopic().getDescribeConfigs(), TimeUnit.MILLISECONDS)
                .entrySet()
                .stream()
                .map(entry -> {
                    String name = entry.getKey().name();
                    Map<String, String> configs = entry.getValue().entries().stream()
                            .filter(configEntry ->
                                    configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                            .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));

                    TopicDescription desc = topicDescriptions.get(name);
                    return Topic.builder()
                            .metadata(Resource.Metadata.builder()
                                    .cluster(managedClusterProperties.getName())
                                    .name(name)
                                    .build())
                            .spec(Topic.TopicSpec.builder()
                                    .replicationFactor(desc.partitions()
                                            .getFirst()
                                            .replicas()
                                            .size())
                                    .partitions(desc.partitions().size())
                                    .configs(configs)
                                    .build())
                            .build();
                })
                .collect(Collectors.toMap(topic -> topic.getMetadata().getName(), Function.identity()));
    }

    /**
     * Create topics.
     *
     * @param toCreate The list of topics to create
     */
    public void createTopics(List<Topic> toCreate) {
        List<NewTopic> newTopics = toCreate.stream()
                .map(topic -> {
                    log.debug(
                            "Creating topic {} on cluster {}.",
                            topic.getMetadata().getName(),
                            topic.getMetadata().getCluster());
                    NewTopic newTopic = new NewTopic(
                            topic.getMetadata().getName(), topic.getSpec().getPartitions(), (short)
                                    topic.getSpec().getReplicationFactor());
                    newTopic.configs(topic.getSpec().getConfigs());
                    return newTopic;
                })
                .toList();

        Map<String, KafkaFuture<Void>> createTopicsResult = managedClusterProperties
                .getAdminClient()
                .createTopics(newTopics)
                .values();

        toCreate.forEach(topicToCreate -> {
            try {
                createTopicsResult
                        .get(topicToCreate.getMetadata().getName())
                        .get(managedClusterProperties.getTimeout().getTopic().getCreate(), TimeUnit.MILLISECONDS);
                topicToCreate.getMetadata().setGeneration(1);
                topicToCreate.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());

                log.info(
                        "Success creating topic {} on cluster {}.",
                        topicToCreate.getMetadata().getName(),
                        managedClusterProperties.getName());
            } catch (InterruptedException e) {
                log.error("Error.", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                topicToCreate
                        .getMetadata()
                        .setStatus(Resource.Metadata.Status.ofFailed("Error while creating topic: " + e.getMessage()));
                log.error(
                        "Error while creating topic {} on cluster {}.",
                        topicToCreate.getMetadata().getName(),
                        managedClusterProperties.getName(),
                        e);
            }

            if (isUnchangedSinceLastApply(topicToCreate)) {
                topicRepository.create(topicToCreate);
            }
        });
    }

    /**
     * Alter topics.
     *
     * @param configChanges The topic config changes
     * @param toUpdate The list of topics to update
     */
    private void alterTopics(Map<ConfigResource, Collection<AlterConfigOp>> configChanges, List<Topic> toUpdate) {
        AlterConfigsResult alterConfigsResult =
                managedClusterProperties.getAdminClient().incrementalAlterConfigs(configChanges);
        alterConfigsResult.values().forEach((key, value) -> {
            Topic updatedTopic = toUpdate.stream()
                    .filter(topic -> topic.getMetadata().getName().equals(key.name()))
                    .findFirst()
                    .get();

            try {
                value.get(managedClusterProperties.getTimeout().getTopic().getAlterConfigs(), TimeUnit.MILLISECONDS);

                updatedTopic
                        .getMetadata()
                        .setGeneration(updatedTopic.getMetadata().getGeneration() + 1);
                updatedTopic.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());

                log.info(
                        "Success updating topic {} configs on cluster {}.",
                        key.name(),
                        managedClusterProperties.getName());
            } catch (InterruptedException e) {
                log.error("Error.", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                updatedTopic
                        .getMetadata()
                        .setStatus(Resource.Metadata.Status.ofFailed(
                                "Error while updating topic configs: " + e.getMessage()));

                log.error(
                        "Error while updating topic {} configs on cluster {}.",
                        updatedTopic.getMetadata().getName(),
                        managedClusterProperties.getName(),
                        e);
            }

            if (isUnchangedSinceLastApply(updatedTopic)) {
                topicRepository.create(updatedTopic);
            }
        });
    }

    /**
     * Delete a list of topics.
     *
     * @param topics The topics to delete
     */
    public void deleteTopics(List<Topic> topics) throws InterruptedException, ExecutionException, TimeoutException {
        List<String> topicsNames =
                topics.stream().map(topic -> topic.getMetadata().getName()).toList();

        managedClusterProperties
                .getAdminClient()
                .deleteTopics(topicsNames)
                .all()
                .get(managedClusterProperties.getTimeout().getTopic().getDelete(), TimeUnit.MILLISECONDS);

        topicsNames.forEach(topicName ->
                log.info("Success deleting topic {} on cluster {}.", topicName, managedClusterProperties.getName()));
    }

    /**
     * Compute the configuration changes.
     *
     * @param configToApply The config from Ns4Kafka
     * @param currentConfig The config from cluster
     * @return A list of config
     */
    private Collection<AlterConfigOp> computeConfigChanges(
            Map<String, String> configToApply, Map<String, String> currentConfig) {
        List<AlterConfigOp> changes = new ArrayList<>();

        configToApply.forEach((key, value) -> {
            if (!currentConfig.containsKey(key)
                    || !TopicConfigUtils.areEquivalent(key, value, currentConfig.get(key))) {
                changes.add(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET));
            }
        });

        currentConfig.forEach((key, value) -> {
            if (!configToApply.containsKey(key)) {
                changes.add(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.DELETE));
            }
        });

        return changes;
    }

    /**
     * Check the topic has been neither deleted nor reapplied since it was read.
     *
     * @param topic The synchronized topic
     * @return True if unchanged, false otherwise
     */
    private boolean isUnchangedSinceLastApply(Topic topic) {
        Optional<Topic> existingTopic = topicRepository.findByName(
                managedClusterProperties.getName(), topic.getMetadata().getName());

        return existingTopic.isPresent()
                && (existingTopic.get().getMetadata().getUpdateTimestamp() == null
                        || (topic.getMetadata().getUpdateTimestamp() != null
                                && !existingTopic
                                        .get()
                                        .getMetadata()
                                        .getUpdateTimestamp()
                                        .after(topic.getMetadata().getUpdateTimestamp())));
    }

    /**
     * For a given topic, get each latest offset by partition in order to delete all the records before these offsets.
     *
     * @param topic The topic to delete records
     * @return A map of offsets by topic-partitions
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, RecordsToDelete> prepareRecordsToDelete(String topic)
            throws ExecutionException, InterruptedException {
        // List all partitions for topic and prepare a listOffsets call
        Map<TopicPartition, OffsetSpec> topicsPartitionsToDelete =
                managedClusterProperties
                        .getAdminClient()
                        .describeTopics(List.of(topic))
                        .allTopicNames()
                        .get()
                        .entrySet()
                        .stream()
                        .flatMap(topicDescriptionEntry -> topicDescriptionEntry.getValue().partitions().stream())
                        .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                        .collect(Collectors.toMap(Function.identity(), _ -> OffsetSpec.latest()));

        // list all latest offsets for each partitions
        return managedClusterProperties
                .getAdminClient()
                .listOffsets(topicsPartitionsToDelete)
                .all()
                .get()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        kv -> RecordsToDelete.beforeOffset(kv.getValue().offset())));
    }

    /**
     * Delete the records for each partition, before each offset.
     *
     * @param recordsToDelete The offsets by topic-partitions
     * @return The new offsets by topic-partitions
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, Long> deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete)
            throws InterruptedException {
        return managedClusterProperties
                .getAdminClient()
                .deleteRecords(recordsToDelete)
                .lowWatermarks()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, kv -> {
                    try {
                        long newValue = kv.getValue().get().lowWatermark();
                        log.info("Deleting records {} of topic-partition {}.", newValue, kv.getKey());
                        return newValue;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Thread interrupted deleting records of topic-partition {}.", kv.getKey(), e);
                        return -1L;
                    } catch (ExecutionException e) {
                        log.error("Execution error deleting records of topic-partition {}.", kv.getKey(), e);
                        return -1L;
                    } catch (Exception e) {
                        log.error("Error deleting records of topic-partition {}.", kv.getKey(), e);
                        return -1L;
                    }
                }));
    }
}
