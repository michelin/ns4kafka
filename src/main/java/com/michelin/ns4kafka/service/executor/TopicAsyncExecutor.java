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

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.TopicRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.service.NamespaceService;
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
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/** Topic executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class TopicAsyncExecutor {
    public static final String CLUSTER_ID = "cluster.id";
    public static final String TOPIC_ENTITY_TYPE = "kafka_topic";
    private final Set<String> ignoredTopics = ConcurrentHashMap.newKeySet();

    private final ManagedClusterProperties managedClusterProperties;
    private final NamespaceService namespaceService;
    private final TopicRepository topicRepository;
    private final SchemaRegistryClient schemaRegistryClient;
    private final Ns4KafkaProperties ns4KafkaProperties;

    private Disposable tagSyncDisposable;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param namespaceService The namespace service
     * @param topicRepository The topic repository
     * @param schemaRegistryClient The schema registry client
     * @param ns4KafkaProperties The Ns4Kafka properties
     */
    public TopicAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            NamespaceService namespaceService,
            TopicRepository topicRepository,
            SchemaRegistryClient schemaRegistryClient,
            Ns4KafkaProperties ns4KafkaProperties) {
        this.managedClusterProperties = managedClusterProperties;
        this.namespaceService = namespaceService;
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
            ignoredTopics.clear();

            Map<String, Topic> brokerTopics = collectBrokerTopics();
            List<Topic> ns4KafkaTopics = topicRepository.findAllForCluster(managedClusterProperties.getName());

            Map<Boolean, List<Topic>> partitioned = ns4KafkaTopics.stream()
                    .collect(Collectors.partitioningBy(topic ->
                            brokerTopics.containsKey(topic.getMetadata().getName())));

            List<Topic> checkTopics = partitioned.get(true);
            List<Topic> createTopics = partitioned.get(false).stream()
                    .filter(topic -> !ignoredTopics.contains(topic.getMetadata().getName()))
                    .toList();

            Map<ConfigResource, Collection<AlterConfigOp>> updateTopics = new HashMap<>();
            checkTopics.forEach(topic -> {
                Map<String, String> actualConf = brokerTopics
                        .get(topic.getMetadata().getName())
                        .getSpec()
                        .getConfigs();

                Collection<AlterConfigOp> changes =
                        computeConfigChanges(topic.getSpec().getConfigs(), actualConf);

                if (!changes.isEmpty()) {
                    ConfigResource cr = new ConfigResource(
                            ConfigResource.Type.TOPIC, topic.getMetadata().getName());
                    updateTopics.put(cr, changes);
                }
            });

            if (managedClusterProperties.isSyncKstreamTopics()) {
                Set<String> ns4KafkaTopicNames = ns4KafkaTopics.stream()
                        .map(topic -> topic.getMetadata().getName())
                        .filter(topic -> topic.endsWith("-changelog") || topic.endsWith("-repartition"))
                        .collect(Collectors.toSet());

                List<Topic> unsyncStreamInternalTopics =
                        getUnsyncStreamsInternalTopics(brokerTopics, ns4KafkaTopicNames);

                if (!unsyncStreamInternalTopics.isEmpty()) {
                    log.atDebug()
                            .addArgument(unsyncStreamInternalTopics.stream()
                                    .map(topic -> topic.getMetadata().getName())
                                    .collect(Collectors.joining(",")))
                            .log("Kafka Streams internal topics(s) to import: {}");

                    importTopics(unsyncStreamInternalTopics);
                }
            }

            if (!createTopics.isEmpty()) {
                log.atDebug()
                        .addArgument("Topic(s) to create: {}")
                        .log(createTopics.stream()
                                .map(topic -> topic.getMetadata().getName())
                                .collect(Collectors.joining(",")));

                createTopics(createTopics);
            }

            if (!updateTopics.isEmpty()) {
                log.atDebug()
                        .addArgument(updateTopics.keySet().stream()
                                .map(ConfigResource::name)
                                .collect(Collectors.joining(",")))
                        .log("Topic(s) to update: {}");

                alterTopics(updateTopics, checkTopics);
            }

            alterCatalogInfo(checkTopics, brokerTopics);
        } catch (ExecutionException | TimeoutException | CancellationException | KafkaStoreException e) {
            log.error("An error occurred during the topic synchronization", e);
        } catch (InterruptedException e) {
            log.error("Thread interrupted during the topic synchronization", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Between broker topics and Ns4Kafka topics, get the unsynchronized Kafka Streams internal topics.
     *
     * @param brokerTopics The topics from the broker
     * @param ns4KafkaTopicNames The topic names from Ns4Kafka
     * @return A list of unsynchronized Kafka Streams internal topics
     */
    private List<Topic> getUnsyncStreamsInternalTopics(
            Map<String, Topic> brokerTopics, Set<String> ns4KafkaTopicNames) {
        List<Namespace> namespaces = namespaceService.findAll();
        return brokerTopics.values().stream()
                // Keep only Kafka Streams internal topics that are not already in Ns4Kafka
                .filter(topic ->
                        !ns4KafkaTopicNames.contains(topic.getMetadata().getName())
                                && (topic.getMetadata().getName().endsWith("-changelog")
                                        || topic.getMetadata().getName().endsWith("-repartition"))
                                && !ignoredTopics.contains(topic.getMetadata().getName()))
                .map(topic -> {
                    // Ignore internal cluster topics. Only keep topics covered by Ns4Kafka.
                    Optional<Namespace> namespace = namespaceService.findByTopicName(
                            namespaces, topic.getMetadata().getName());
                    if (namespace.isEmpty()) {
                        log.trace(
                                "No namespace found for topic {}. Skipping import.",
                                topic.getMetadata().getName());
                        return null;
                    }

                    topic.getMetadata()
                            .setNamespace(namespace.get().getMetadata().getName());

                    return topic;
                })
                .filter(Objects::nonNull)
                .toList();
    }

    /**
     * Import unsynchronized topics from broker to Ns4Kafka.
     *
     * @param topics The list of topics to import
     */
    public void importTopics(List<Topic> topics) {
        topics.forEach(topic -> {
            topic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            topic.getMetadata().setCluster(managedClusterProperties.getName());
            topic.setStatus(Topic.TopicStatus.ofSuccess("Imported from cluster"));

            topicRepository.create(topic);

            log.atInfo()
                    .addArgument(topic.getMetadata().getName())
                    .addArgument(managedClusterProperties.getName())
                    .log("Success importing topic {} on {}.");
        });
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

        // Add topics to blacklist so the Kstream internal topics are not imported after deletion
        // This could happen if such topic is deleted after the broker topics are listed
        // but before the Ns4Kafka topics are listed during synchronization
        if (managedClusterProperties.isSyncKstreamTopics()) {
            ignoredTopics.addAll(topicsNames);
        }

        log.atInfo()
                .addArgument(String.join(", ", topicsNames))
                .addArgument(managedClusterProperties.getName())
                .log("Success deleting topics {} on cluster {}.");
    }

    /**
     * Collect all topics on broker.
     *
     * @return All topics by name
     */
    public Map<String, Topic> collectBrokerTopics() throws ExecutionException, InterruptedException, TimeoutException {
        return collectBrokerTopicsFromNames(listBrokerTopicNames());
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
            descriptionResponse.data().kafkaTopic().forEach(describedTopic -> topics.get(describedTopic.name())
                    .getSpec()
                    .setDescription(describedTopic.description()));
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
            tagsResponse.data().kafkaTopic().forEach(taggedTopic -> topics.get(taggedTopic.name())
                    .getSpec()
                    .setTags(taggedTopic.tags()));
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

        Map<String, Topic> topics = managedClusterProperties
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
                            .metadata(Metadata.builder()
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

        enrichWithCatalogInfo(topics);

        return topics;
    }

    /**
     * Alter topics.
     *
     * @param toUpdate The topics to update
     * @param topics The current topics
     */
    private void alterTopics(Map<ConfigResource, Collection<AlterConfigOp>> toUpdate, List<Topic> topics) {
        AlterConfigsResult alterConfigsResult =
                managedClusterProperties.getAdminClient().incrementalAlterConfigs(toUpdate);
        alterConfigsResult.values().forEach((key, value) -> {
            Topic updatedTopic = topics.stream()
                    .filter(topic -> topic.getMetadata().getName().equals(key.name()))
                    .findFirst()
                    .get();

            try {
                value.get(managedClusterProperties.getTimeout().getTopic().getAlterConfigs(), TimeUnit.MILLISECONDS);
                updatedTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                updatedTopic
                        .getMetadata()
                        .setGeneration(updatedTopic.getMetadata().getGeneration() + 1);
                updatedTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic configs updated"));

                log.atInfo()
                        .addArgument(key.name())
                        .addArgument(managedClusterProperties.getName())
                        .addArgument(toUpdate.get(key).stream()
                                .map(AlterConfigOp::toString)
                                .collect(Collectors.joining(",")))
                        .log("Success updating topic configs {} on cluster {}: [{}]");
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                updatedTopic.setStatus(
                        Topic.TopicStatus.ofFailed("Error while updating topic configs: " + e.getMessage()));
                log.error(
                        "Error while updating topic configs {} on cluster {}",
                        key.name(),
                        managedClusterProperties.getName(),
                        e);
            }

            topicRepository.create(updatedTopic);
        });
    }

    /**
     * Create topics.
     *
     * @param topics The topics to create
     */
    private void createTopics(List<Topic> topics) {
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

        CreateTopicsResult createTopicsResult =
                managedClusterProperties.getAdminClient().createTopics(newTopics);
        createTopicsResult.values().forEach((key, value) -> {
            Topic createdTopic = topics.stream()
                    .filter(t -> t.getMetadata().getName().equals(key))
                    .findFirst()
                    .get();

            try {
                value.get(managedClusterProperties.getTimeout().getTopic().getCreate(), TimeUnit.MILLISECONDS);
                createdTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                createdTopic.getMetadata().setGeneration(1);
                createdTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic created"));
                log.info("Success creating topic {} on cluster {}", key, managedClusterProperties.getName());
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                createdTopic.setStatus(Topic.TopicStatus.ofFailed("Error while creating topic: " + e.getMessage()));
                log.error("Error while creating topic {} on cluster {}", key, managedClusterProperties.getName(), e);
            }
            topicRepository.create(createdTopic);
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
     * @param expected The config from Ns4Kafka
     * @param actual The config from cluster
     * @return A list of config
     */
    private Collection<AlterConfigOp> computeConfigChanges(Map<String, String> expected, Map<String, String> actual) {
        List<AlterConfigOp> changes = new ArrayList<>();

        expected.forEach((key, value) -> {
            if (!actual.containsKey(key) || !value.equals(actual.get(key))) {
                changes.add(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET));
            }
        });

        actual.forEach((key, value) -> {
            if (!expected.containsKey(key)) {
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
                        var newValue = kv.getValue().get().lowWatermark();
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
}
