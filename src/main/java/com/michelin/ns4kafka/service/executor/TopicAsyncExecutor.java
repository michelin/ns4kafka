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
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.TopicRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.service.TopicService;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.service.client.schema.entities.GraphQueryResponse;
import com.michelin.ns4kafka.service.client.schema.entities.TagInfo;
import com.michelin.ns4kafka.service.client.schema.entities.TagTopicInfo;
import com.michelin.ns4kafka.service.client.schema.entities.TopicDescriptionUpdateAttributes;
import com.michelin.ns4kafka.service.client.schema.entities.TopicDescriptionUpdateBody;
import com.michelin.ns4kafka.service.client.schema.entities.TopicDescriptionUpdateEntity;
import com.michelin.ns4kafka.service.client.schema.entities.TopicListResponse;
import io.micronaut.context.annotation.EachBean;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/** Topic executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class TopicAsyncExecutor {
    public static final String CLUSTER_ID = "cluster.id";
    public static final String TOPIC_ENTITY_TYPE = "kafka_topic";
    public static final String ERROR = "Error";

    private final ManagedClusterProperties managedClusterProperties;
    private final TopicService topicService;
    private final TopicRepository topicRepository;
    private final SchemaRegistryClient schemaRegistryClient;
    private final Ns4KafkaProperties ns4KafkaProperties;

    private Disposable tagSyncDisposable;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param topicRepository The topic repository
     * @param schemaRegistryClient The schema registry client
     * @param ns4KafkaProperties The Ns4Kafka properties
     */
    public TopicAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            TopicService topicService,
            TopicRepository topicRepository,
            SchemaRegistryClient schemaRegistryClient,
            Ns4KafkaProperties ns4KafkaProperties) {
        this.managedClusterProperties = managedClusterProperties;
        this.topicService = topicService;
        this.topicRepository = topicRepository;
        this.schemaRegistryClient = schemaRegistryClient;
        this.ns4KafkaProperties = ns4KafkaProperties;
    }

    /** Run the topic synchronization. */
    public void run() {
        if (managedClusterProperties.isManageTopics()) {
            synchronizeTopics();
        }
    }

    /** Start the topic synchronization. */
    public void synchronizeTopics() {
        log.debug("Starting topic collection for cluster {}", managedClusterProperties.getName());

        try {
            Map<Boolean, List<Topic>> partitioned =
                    topicService.findAllToDeployForCluster(managedClusterProperties.getName()).stream()
                            .collect(Collectors.partitioningBy(Resource::isCreated));
            List<Topic> topicsToCreate = partitioned.get(false);
            List<Topic> topicsToUpdate = partitioned.get(true);
            List<Topic> topicsToDelete = topicService.findAllToDeleteForCluster(managedClusterProperties.getName());

            if (!topicsToCreate.isEmpty()) {
                log.atDebug()
                        .addArgument(topicsToCreate.stream()
                                .map(topic -> topic.getMetadata().getName())
                                .collect(Collectors.joining(",")))
                        .log("Topic(s) to create: {}");
                createTopics(topicsToCreate);
            }

            if (!topicsToUpdate.isEmpty()) {
                log.atDebug()
                        .addArgument(topicsToUpdate.stream()
                                .map(topic -> topic.getMetadata().getName())
                                .collect(Collectors.joining(",")))
                        .log("Topic(s) to update: {}");

                List<String> topicsNames = topicsToUpdate.stream()
                        .map(topic -> topic.getMetadata().getName())
                        .toList();
                alterTopics(topicsToUpdate, collectBrokerTopicsFromNames(topicsNames));
            }

            if (!topicsToDelete.isEmpty()) {
                log.atDebug()
                        .addArgument(topicsToDelete.stream()
                                .map(topic -> topic.getMetadata().getName())
                                .collect(Collectors.joining(",")))
                        .log("Topic(s) to delete: {}");

                deleteTopics(topicsToDelete);
            }

        } catch (InterruptedException e) {
            log.error("Exception ", e);
            Thread.currentThread().interrupt();
        } catch (CancellationException | KafkaStoreException | ExecutionException | TimeoutException e) {
            log.error("An error occurred during the topic synchronization", e);
        }
    }

    /**
     * Alter catalog info.
     *
     * @param checkTopics Topics from Ns4Kafka
     * @param brokerTopics Topics from broker
     */
    public void alterCatalogInfo(List<Topic> checkTopics, Map<String, Topic> brokerTopics) {
        if (ns4KafkaProperties.getConfluentCloud().getStreamCatalog().isSyncCatalog()
                && managedClusterProperties.isConfluentCloud()) {
            alterTags(checkTopics, brokerTopics);
            alterDescriptions(checkTopics, brokerTopics);
        }
    }

    /**
     * Alter tags.
     *
     * @param ns4kafkaTopics Topics from Ns4Kafka
     * @param brokerTopics Topics from broker
     */
    public void alterTags(List<Topic> ns4kafkaTopics, Map<String, Topic> brokerTopics) {
        Map<Topic, List<TagTopicInfo>> topicTagsMapping = ns4kafkaTopics.stream()
                .map(topic -> {
                    Topic brokerTopic = brokerTopics.get(topic.getMetadata().getName());

                    dissociateTags(brokerTopic, topic);

                    Set<String> newTags = topic.getSpec().getTags().stream()
                            .filter(tag -> !brokerTopic.getSpec().getTags().contains(tag))
                            .collect(Collectors.toSet());

                    return new AbstractMap.SimpleEntry<>(
                            topic,
                            newTags.stream()
                                    .map(tag -> TagTopicInfo.builder()
                                            .entityName(managedClusterProperties
                                                            .getConfig()
                                                            .getProperty(CLUSTER_ID) + ":"
                                                    + topic.getMetadata().getName())
                                            .typeName(tag)
                                            .entityType(TOPIC_ENTITY_TYPE)
                                            .build())
                                    .toList());
                })
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        if (!topicTagsMapping.isEmpty()) {
            tagSyncDisposable = createAndAssociateTags(topicTagsMapping);
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
     * Alter description.
     *
     * @param ns4kafkaTopics Topics from ns4kafka
     * @param brokerTopics Topics from broker
     */
    public void alterDescriptions(List<Topic> ns4kafkaTopics, Map<String, Topic> brokerTopics) {
        String clusterId = managedClusterProperties.getConfig().getProperty(CLUSTER_ID);

        for (Topic topic : ns4kafkaTopics) {
            String description = topic.getSpec().getDescription();
            String brokerDescription =
                    brokerTopics.get(topic.getMetadata().getName()).getSpec().getDescription();

            if (!Objects.equals(brokerDescription, description)) {
                String qualifiedName = clusterId + ":" + topic.getMetadata().getName();

                TopicDescriptionUpdateBody body = TopicDescriptionUpdateBody.builder()
                        .entity(TopicDescriptionUpdateEntity.builder()
                                .typeName(TOPIC_ENTITY_TYPE)
                                .attributes(TopicDescriptionUpdateAttributes.builder()
                                        .qualifiedName(qualifiedName)
                                        .description(description)
                                        .build())
                                .build())
                        .build();

                schemaRegistryClient
                        .updateDescription(managedClusterProperties.getName(), body)
                        .subscribe(
                                _ -> {
                                    log.info(
                                            "Success update description {}",
                                            qualifiedName + ": "
                                                    + topic.getSpec().getDescription());
                                    topic.getMetadata()
                                            .setGeneration(topic.getMetadata().getGeneration() + 1);
                                    topic.setStatus(Topic.TopicStatus.ofSuccess("Topic description updated"));
                                    topicRepository.create(topic);
                                },
                                error -> {
                                    log.error(
                                            "Error update description {}",
                                            qualifiedName + ": "
                                                    + topic.getSpec().getDescription(),
                                            error);
                                    topic.setStatus(Topic.TopicStatus.ofFailed(
                                            "Error while updating topic description: " + error.getMessage()));
                                    topicRepository.create(topic);
                                });
            }
        }
    }

    /**
     * Enrich topics with Confluent catalog info.
     *
     * @param topics Topics to enrich
     */
    public void enrichWithCatalogInfo(Map<String, Topic> topics) {
        if (ns4KafkaProperties.getConfluentCloud().getStreamCatalog().isSyncCatalog()
                && managedClusterProperties.isConfluentCloud()) {
            try {
                enrichWithGraphQlCatalogInfo(topics);
            } catch (Exception graphQLError) {
                try {
                    enrichWithRestCatalogInfo(topics);
                } catch (Exception restError) {
                    log.error(
                            "Error while enriching topics with catalog info:\r\nGraphQL error: {}\r\nRest API error: {}",
                            graphQLError.getMessage(),
                            restError.getMessage());
                }
            }
        }
    }

    /**
     * Enrich topics with Confluent catalog information, using Confluent Stream Catalog GraphQL API.
     *
     * @param topics Topics to enrich
     */
    public void enrichWithGraphQlCatalogInfo(Map<String, Topic> topics) {
        // block tags & description enrichment to make sure they are present during NS4 & broker comparison
        GraphQueryResponse descriptionResponse = schemaRegistryClient
                .getTopicsWithDescriptionWithGraphQl(managedClusterProperties.getName())
                .block();

        if (descriptionResponse != null
                && descriptionResponse.data() != null
                && descriptionResponse.data().kafkaTopic() != null) {
            descriptionResponse
                    .data()
                    .kafkaTopic()
                    .forEach(describedTopic ->
                            topics.get(describedTopic.name()).getSpec().setDescription(describedTopic.description()));
        }

        GraphQueryResponse tagsResponse = schemaRegistryClient
                .listTags(managedClusterProperties.getName())
                .flatMap(tagsList -> tagsList.isEmpty()
                        ? Mono.just(GraphQueryResponse.builder().data(null).build())
                        : schemaRegistryClient.getTopicsWithTagsWithGraphQl(
                                managedClusterProperties.getName(),
                                tagsList.stream()
                                        .map(tagInfo -> "\"" + tagInfo.name() + "\"")
                                        .toList()))
                .block();

        if (tagsResponse != null
                && tagsResponse.data() != null
                && tagsResponse.data().kafkaTopic() != null) {
            tagsResponse
                    .data()
                    .kafkaTopic()
                    .forEach(taggedTopic ->
                            topics.get(taggedTopic.name()).getSpec().setTags(taggedTopic.tags()));
        }
    }

    /**
     * Enrich topics with Confluent catalog information, using Confluent Stream Catalog REST API.
     *
     * @param topics Topics to enrich
     */
    public void enrichWithRestCatalogInfo(Map<String, Topic> topics) {
        // getting topics by managing offset & limit
        TopicListResponse topicListResponse;
        int offset = 0;
        int limit = ns4KafkaProperties.getConfluentCloud().getStreamCatalog().getPageSize();

        do {
            topicListResponse = schemaRegistryClient
                    .getTopicsWithStreamCatalog(managedClusterProperties.getName(), limit, offset)
                    .block();
            if (topicListResponse == null) {
                break;
            }
            topicListResponse.entities().stream()
                    .filter(topicEntity -> (topicEntity.attributes().description() != null
                                    || !topicEntity.classificationNames().isEmpty())
                            && topics.containsKey(topicEntity.attributes().name()))
                    .forEach(topicEntity -> {
                        topics.get(topicEntity.attributes().name())
                                .getSpec()
                                .setTags(topicEntity.classificationNames());
                        topics.get(topicEntity.attributes().name())
                                .getSpec()
                                .setDescription(topicEntity.attributes().description());
                    });
            offset += limit;
        } while (!topicListResponse.entities().isEmpty());
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
     * @param topics The topics to create
     */
    public void createTopics(List<Topic> topics) {
        List<NewTopic> newTopics = topics.stream()
                .map(topic -> {
                    log.debug(
                            "Creating topic {} on cluster {}",
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

        topics.forEach(topicToCreate -> {
            try {
                createTopicsResult
                        .get(topicToCreate.getMetadata().getName())
                        .get(managedClusterProperties.getTimeout().getTopic().getCreate(), TimeUnit.MILLISECONDS);

                Optional<Topic> existingTopic = topicService.findByName(
                        topicToCreate.getMetadata().getCluster(),
                        topicToCreate.getMetadata().getName());
                Topic lastVersion = existingTopic.orElse(topicToCreate);
                lastVersion.getMetadata().setGeneration(1);

                boolean isUnchangedSinceLastApply = existingTopic.isEmpty()
                        || !existingTopic
                                .get()
                                .getMetadata()
                                .getUpdateTimestamp()
                                .after(topicToCreate.getMetadata().getUpdateTimestamp());

                if (isUnchangedSinceLastApply) {
                    lastVersion.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                }

                topicRepository.create(lastVersion);

                log.info(
                        "Success creating topic {} on cluster {}",
                        topicToCreate.getMetadata().getName(),
                        managedClusterProperties.getName());
            } catch (InterruptedException e) {
                log.error(ERROR, e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (isUnchangedSinceLastApply(topicToCreate)) {
                    if (e.getCause() instanceof TopicExistsException) {
                        // Let the next executor update topic, because if update here with alterTopics, we would need
                        // collectBrokerTopicsFromNames which can throw errors we don't want to handle in createTopics
                        topicToCreate.getMetadata().setStatus(Resource.Metadata.Status.ofPending());
                        topicToCreate.getMetadata().setGeneration(1);
                        topicRepository.create(topicToCreate);
                        return;
                    }

                    topicToCreate
                            .getMetadata()
                            .setStatus(
                                    Resource.Metadata.Status.ofFailed("Error while creating topic: " + e.getMessage()));
                    topicRepository.create(topicToCreate);
                    log.error(
                            "Error while creating topic {} on cluster {}",
                            topicToCreate.getMetadata().getName(),
                            managedClusterProperties.getName(),
                            e);
                }
            }
        });
    }

    /**
     * Alter topics.
     *
     * @param targetTopics The target topics
     * @param brokerTopics The current topics
     */
    public void alterTopics(List<Topic> targetTopics, Map<String, Topic> brokerTopics) {
        Map<ConfigResource, Collection<AlterConfigOp>> topicConfigsToUpdate = targetTopics.stream()
                .collect(Collectors.toMap(
                        topic -> new ConfigResource(
                                ConfigResource.Type.TOPIC, topic.getMetadata().getName()),
                        topic -> {
                            Map<String, String> currentConfig = brokerTopics
                                    .get(topic.getMetadata().getName())
                                    .getSpec()
                                    .getConfigs();

                            return computeConfigChanges(topic.getSpec().getConfigs(), currentConfig);
                        }));

        AlterConfigsResult alterConfigsResult =
                managedClusterProperties.getAdminClient().incrementalAlterConfigs(topicConfigsToUpdate);
        alterConfigsResult.values().forEach((key, value) -> {
            Topic updatedTopic = targetTopics.stream()
                    .filter(topic -> topic.getMetadata().getName().equals(key.name()))
                    .findFirst()
                    .get();

            try {
                value.get(managedClusterProperties.getTimeout().getTopic().getAlterConfigs(), TimeUnit.MILLISECONDS);

                if (isUnchangedSinceLastApply(updatedTopic)) {
                    updatedTopic
                            .getMetadata()
                            .setGeneration(updatedTopic.getMetadata().getGeneration() + 1);
                    updatedTopic.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                    topicRepository.create(updatedTopic);

                    log.atInfo()
                            .addArgument(key.name())
                            .addArgument(managedClusterProperties.getName())
                            .addArgument(topicConfigsToUpdate.get(key).stream()
                                    .map(AlterConfigOp::toString)
                                    .collect(Collectors.joining(",")))
                            .log("Success updating topic configs {} on cluster {}: [{}]");
                }
            } catch (InterruptedException e) {
                log.error(ERROR, e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (isUnchangedSinceLastApply(updatedTopic)) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        createTopics(List.of(updatedTopic));
                        return;
                    }
                    updatedTopic
                            .getMetadata()
                            .setStatus(Resource.Metadata.Status.ofFailed(
                                    "Error while updating topic configs: " + e.getMessage()));
                    topicRepository.create(updatedTopic);

                    log.error(
                            "Error while updating topic configs {} on cluster {}",
                            updatedTopic.getMetadata().getName(),
                            managedClusterProperties.getName(),
                            e);
                }
            }
        });
    }

    /**
     * Delete a list of topics.
     *
     * @param topics The topics to delete
     */
    public void deleteTopics(List<Topic> topics) {
        List<String> topicsNames =
                topics.stream().map(topic -> topic.getMetadata().getName()).toList();

        Map<String, KafkaFuture<Void>> deletedTopicsResult = managedClusterProperties
                .getAdminClient()
                .deleteTopics(topicsNames)
                .topicNameValues();

        topics.forEach(topicToDelete -> {
            try {
                deletedTopicsResult
                        .get(topicToDelete.getMetadata().getName())
                        .get(managedClusterProperties.getTimeout().getTopic().getDelete(), TimeUnit.MILLISECONDS);

                if (isUnchangedSinceLastApply(topicToDelete)) {
                    log.atInfo()
                            .addArgument(topicToDelete.getMetadata().getName())
                            .addArgument(managedClusterProperties.getName())
                            .log("Success deleting topic {} on cluster {}.");
                    topicRepository.delete(topicToDelete);
                }
            } catch (InterruptedException e) {
                log.error(ERROR, e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (isUnchangedSinceLastApply(topicToDelete)) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        log.info(
                                "Topic {} does not exist on the cluster {}: Topic will be removed from Ns4Kafka.",
                                topicToDelete.getMetadata().getName(),
                                managedClusterProperties.getName());
                        topicRepository.delete(topicToDelete);
                        return;
                    }

                    topicToDelete
                            .getMetadata()
                            .setStatus(
                                    Resource.Metadata.Status.ofFailed("Error while deleting topic: " + e.getMessage()));
                    topicRepository.create(topicToDelete);

                    log.error(
                            "Error while deleting topic {} on cluster {}",
                            topicToDelete.getMetadata().getName(),
                            managedClusterProperties.getName(),
                            e);
                }
            }
        });
    }

    /**
     * Create tags and associate them.
     *
     * @param topicTags Mapping between topics and their list of tags info
     * @return A disposable to subscribe to the creation and association of tags
     */
    public Disposable createAndAssociateTags(Map<Topic, List<TagTopicInfo>> topicTags) {
        List<TagTopicInfo> tagsToAssociate =
                topicTags.values().stream().flatMap(Collection::stream).toList();

        List<TagInfo> tagsToCreate = tagsToAssociate.stream()
                .map(tag -> TagInfo.builder().name(tag.typeName()).build())
                .distinct()
                .toList();

        String tagsListString = tagsToCreate.stream().map(TagInfo::name).collect(Collectors.joining(","));

        return schemaRegistryClient
                .createTags(managedClusterProperties.getName(), tagsToCreate)
                .subscribe(
                        _ -> {
                            log.info("Success creating tag {}.", tagsListString);

                            schemaRegistryClient
                                    .associateTags(managedClusterProperties.getName(), tagsToAssociate)
                                    .subscribe(
                                            _ -> topicTags.forEach((topic, tags) -> {
                                                log.info(
                                                        "Success associating tag {}.",
                                                        managedClusterProperties
                                                                        .getConfig()
                                                                        .getProperty(CLUSTER_ID) + ":"
                                                                + topic.getMetadata()
                                                                        .getName() + "/"
                                                                + tags.stream()
                                                                        .map(TagTopicInfo::typeName)
                                                                        .collect(Collectors.joining(",")));
                                                topic.getMetadata()
                                                        .setGeneration(topic.getMetadata()
                                                                        .getGeneration()
                                                                + 1);
                                                topic.setStatus(Topic.TopicStatus.ofSuccess("Topic tags updated"));
                                                topicRepository.create(topic);
                                            }),
                                            error -> topicTags.forEach((topic, tags) -> {
                                                log.error(
                                                        "Error associating tag {}.",
                                                        managedClusterProperties
                                                                        .getConfig()
                                                                        .getProperty(CLUSTER_ID)
                                                                + ":"
                                                                + topic.getMetadata()
                                                                        .getName() + "/"
                                                                + tags.stream()
                                                                        .map(TagTopicInfo::typeName)
                                                                        .collect(Collectors.joining(",")),
                                                        error);
                                                topic.setStatus(Topic.TopicStatus.ofFailed(
                                                        "Error while associating topic tags: " + error.getMessage()));
                                                topicRepository.create(topic);
                                            }));
                        },
                        error -> log.error("Error creating tag {}.", tagsListString, error));
    }

    /**
     * Dissociate tags to a topic.
     *
     * @param brokerTopic The topic from the broker
     * @param topic The topic from Ns4Kafka
     */
    private void dissociateTags(Topic brokerTopic, Topic topic) {
        brokerTopic.getSpec().getTags().stream()
                .filter(tag -> !topic.getSpec().getTags().contains(tag))
                .forEach(tag -> schemaRegistryClient
                        .dissociateTag(
                                managedClusterProperties.getName(),
                                managedClusterProperties.getConfig().getProperty(CLUSTER_ID) + ":"
                                        + topic.getMetadata().getName(),
                                tag)
                        .subscribe(
                                _ -> {
                                    log.info(
                                            "Success dissociating tag {}.",
                                            managedClusterProperties.getConfig().getProperty(CLUSTER_ID) + ":"
                                                    + topic.getMetadata().getName() + "/" + tag);
                                    topic.getMetadata()
                                            .setGeneration(topic.getMetadata().getGeneration() + 1);
                                    topic.setStatus(Topic.TopicStatus.ofSuccess("Topic tags updated"));
                                    topicRepository.create(topic);
                                },
                                error -> {
                                    log.error(
                                            "Error dissociating tag {}.",
                                            managedClusterProperties.getConfig().getProperty(CLUSTER_ID) + ":"
                                                    + topic.getMetadata().getName() + "/" + tag,
                                            error);
                                    topic.setStatus(Topic.TopicStatus.ofFailed(
                                            "Error while dissociating topic tags: " + error.getMessage()));
                                    topicRepository.create(topic);
                                }));
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
            if (!currentConfig.containsKey(key) || !value.equals(currentConfig.get(key))) {
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
                        log.info("Deleting records {} of topic-partition {}", newValue, kv.getKey());
                        return newValue;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Thread interrupted deleting records of topic-partition {}", kv.getKey(), e);
                        return -1L;
                    } catch (ExecutionException e) {
                        log.error("Execution error deleting records of topic-partition {}", kv.getKey(), e);
                        return -1L;
                    } catch (Exception e) {
                        log.error("Error deleting records of topic-partition {}", kv.getKey(), e);
                        return -1L;
                    }
                }));
    }

    /** Dispose the topic synchronization disposable when the bean is destroyed. */
    @PreDestroy
    public void onDestroy() {
        if (tagSyncDisposable != null && !tagSyncDisposable.isDisposed()) {
            tagSyncDisposable.dispose();
        }
    }

    /**
     * Checks whether the topic has been reapplied since the last deployment. Avoids publishing over a topic that has
     * already been changed.
     *
     * @param topic The deployed or deleted topic
     * @return True if it has been reapplied, false otherwise
     */
    private boolean isUnchangedSinceLastApply(Topic topic) {
        Optional<Topic> existingTopic = topicService.findByName(
                topic.getMetadata().getCluster(), topic.getMetadata().getName());

        return existingTopic.isEmpty()
                || !existingTopic
                        .get()
                        .getMetadata()
                        .getUpdateTimestamp()
                        .after(topic.getMetadata().getUpdateTimestamp());
    }
}
