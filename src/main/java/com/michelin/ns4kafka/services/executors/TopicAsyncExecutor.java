package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.TagEntities;
import com.michelin.ns4kafka.services.clients.schema.entities.TagEntity;
import com.michelin.ns4kafka.services.clients.schema.entities.TagInfo;
import com.michelin.ns4kafka.services.clients.schema.entities.TagTopicInfo;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
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

/**
 * Topic executor.
 */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
@AllArgsConstructor
public class TopicAsyncExecutor {
    public static final String CLUSTER_ID = "cluster.id";
    public static final String TOPIC_ENTITY_TYPE = "kafka_topic";

    private final ManagedClusterProperties managedClusterProperties;

    private TopicRepository topicRepository;

    private SchemaRegistryClient schemaRegistryClient;

    private Admin getAdminClient() {
        return managedClusterProperties.getAdminClient();
    }

    /**
     * Run the topic synchronization.
     */
    public void run() {
        if (this.managedClusterProperties.isManageTopics()) {
            synchronizeTopics();
        }
    }

    /**
     * Start the topic synchronization.
     */
    public void synchronizeTopics() {
        log.debug("Starting topic collection for cluster {}", managedClusterProperties.getName());

        try {
            Map<String, Topic> brokerTopics = collectBrokerTopics();
            List<Topic> ns4kafkaTopics = topicRepository.findAllForCluster(managedClusterProperties.getName());

            List<Topic> createTopics = ns4kafkaTopics.stream()
                .filter(topic -> !brokerTopics.containsKey(topic.getMetadata().getName()))
                .toList();

            List<Topic> checkTopics = ns4kafkaTopics.stream()
                .filter(topic -> brokerTopics.containsKey(topic.getMetadata().getName()))
                .toList();

            Map<ConfigResource, Collection<AlterConfigOp>> updateTopics = checkTopics.stream()
                .map(topic -> {
                    Map<String, String> actualConf =
                        brokerTopics.get(topic.getMetadata().getName()).getSpec().getConfigs();
                    Map<String, String> expectedConf =
                        topic.getSpec().getConfigs() == null ? Map.of() : topic.getSpec().getConfigs();
                    Collection<AlterConfigOp> topicConfigChanges = computeConfigChanges(expectedConf, actualConf);
                    if (!topicConfigChanges.isEmpty()) {
                        ConfigResource cr =
                            new ConfigResource(ConfigResource.Type.TOPIC, topic.getMetadata().getName());
                        return Map.entry(cr, topicConfigChanges);
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!createTopics.isEmpty()) {
                log.debug("Topic(s) to create: "
                    + String.join(", ", createTopics.stream().map(topic -> topic.getMetadata().getName()).toList()));
            }

            if (!updateTopics.isEmpty()) {
                log.debug("Topic(s) to update: "
                    + String.join(", ", updateTopics.keySet().stream().map(ConfigResource::name).toList()));
                for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> e : updateTopics.entrySet()) {
                    for (AlterConfigOp op : e.getValue()) {
                        log.debug(
                            e.getKey().name() + " " + op.opType().toString() + " " + op.configEntry().name() + "("
                                + op.configEntry().value() + ")");
                    }
                }
            }

            createTopics(createTopics);
            alterTopics(updateTopics, checkTopics);

            if (managedClusterProperties.isConfluentCloud()) {
                alterTags(checkTopics, brokerTopics);
            }
        } catch (ExecutionException | TimeoutException | CancellationException | KafkaStoreException e) {
            log.error("An error occurred during the topic synchronization", e);
        } catch (InterruptedException e) {
            log.error("Thread interrupted during the topic synchronization", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Alter tags.
     *
     * @param ns4kafkaTopics Topics from ns4kafka
     * @param brokerTopics   Topics from broker
     */
    public void alterTags(List<Topic> ns4kafkaTopics, Map<String, Topic> brokerTopics) {
        List<TagTopicInfo> tagsToCreate = ns4kafkaTopics
            .stream()
            .flatMap(topic -> {
                Topic brokerTopic = brokerTopics.get(topic.getMetadata().getName());

                // Get tags to delete
                Set<String> existingTags = new HashSet<>(brokerTopic.getSpec().getTags());
                existingTags.removeAll(Set.copyOf(topic.getSpec().getTags()));
                dissociateTags(existingTags, topic.getMetadata().getName());

                // Get tags to create
                Set<String> newTags = new HashSet<>(topic.getSpec().getTags());
                newTags.removeAll(Set.copyOf(brokerTopic.getSpec().getTags()));

                return newTags
                    .stream()
                    .map(tag -> TagTopicInfo.builder()
                        .entityName(managedClusterProperties
                            .getConfig()
                            .getProperty(CLUSTER_ID) + ":" + topic.getMetadata().getName())
                        .typeName(tag)
                        .entityType(TOPIC_ENTITY_TYPE)
                        .build());
            })
            .toList();

        if (!tagsToCreate.isEmpty()) {
            createAndAssociateTags(tagsToCreate);
        }
    }

    /**
     * Delete a topic.
     *
     * @param topic The topic to delete
     */
    public void deleteTopic(Topic topic) throws InterruptedException, ExecutionException, TimeoutException {
        getAdminClient().deleteTopics(List.of(topic.getMetadata().getName()))
            .all()
            .get(30, TimeUnit.SECONDS);

        log.info("Success deleting topic {} on {}", topic.getMetadata().getName(),
            managedClusterProperties.getName());

        if (managedClusterProperties.isConfluentCloud() && !topic.getSpec().getTags().isEmpty()) {
            dissociateTags(topic.getSpec().getTags(), topic.getMetadata().getName());
        }
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
        return getAdminClient().listTopics().listings()
            .get(30, TimeUnit.SECONDS)
            .stream()
            .map(TopicListing::name)
            .toList();
    }

    /**
     * Enrich topics with confluent tags.
     *
     * @param topics Topics to complete
     */
    public void enrichWithTags(Map<String, Topic> topics) {
        if (managedClusterProperties.isConfluentCloud()) {
            TagEntities tagEntities = schemaRegistryClient.getTopicWithTags(managedClusterProperties.getName()).block();
            if (tagEntities != null) {
                topics.forEach((key, value) -> {
                    Optional<List<String>> tags = tagEntities.entities().stream()
                        .filter(tagEntity -> value.getMetadata().getName().equals(tagEntity.displayText()))
                        .map(TagEntity::classificationNames).findFirst();
                    value.getSpec().setTags(tags.orElse(Collections.emptyList()));
                });
            }
        }
    }

    /**
     * Collect all topics on broker from a list of topic names.
     *
     * @param topicNames The topic names
     * @return All topics by name
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException   Any execution exception
     * @throws TimeoutException     Any timeout exception
     */
    public Map<String, Topic> collectBrokerTopicsFromNames(List<String> topicNames)
        throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, TopicDescription> topicDescriptions = getAdminClient().describeTopics(topicNames)
            .allTopicNames().get();

        Map<String, Topic> topics = getAdminClient()
            .describeConfigs(topicNames.stream()
                .map(s -> new ConfigResource(ConfigResource.Type.TOPIC, s))
                .toList())
            .all()
            .get(30, TimeUnit.SECONDS)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                configResourceConfigEntry -> configResourceConfigEntry.getKey().name(),
                configResourceConfigEntry -> configResourceConfigEntry.getValue().entries()
                    .stream()
                    .filter(configEntry -> configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                    .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))
            ))
            .entrySet()
            .stream()
            .map(stringMapEntry -> Topic.builder()
                .metadata(Metadata.builder()
                    .cluster(managedClusterProperties.getName())
                    .name(stringMapEntry.getKey())
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .replicationFactor(
                        topicDescriptions.get(stringMapEntry.getKey()).partitions().get(0).replicas().size())
                    .partitions(topicDescriptions.get(stringMapEntry.getKey()).partitions().size())
                    .configs(stringMapEntry.getValue())
                    .build())
                .build()
            )
            .collect(Collectors.toMap(topic -> topic.getMetadata().getName(), Function.identity()));

        enrichWithTags(topics);

        return topics;
    }

    /**
     * Alter topics.
     *
     * @param toUpdate The topics to update
     * @param topics   The current topics
     */
    private void alterTopics(Map<ConfigResource, Collection<AlterConfigOp>> toUpdate, List<Topic> topics) {
        AlterConfigsResult alterConfigsResult = getAdminClient().incrementalAlterConfigs(toUpdate);
        alterConfigsResult.values().forEach((key, value) -> {
            Topic updatedTopic = topics
                .stream()
                .filter(t -> t.getMetadata().getName().equals(key.name()))
                .findFirst()
                .get();

            try {
                value.get(10, TimeUnit.SECONDS);
                updatedTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                updatedTopic.getMetadata().setGeneration(updatedTopic.getMetadata().getGeneration() + 1);
                updatedTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic configs updated"));

                log.info("Success updating topic configs {} on {}: [{}]",
                    key.name(),
                    managedClusterProperties.getName(),
                    toUpdate.get(key).stream().map(AlterConfigOp::toString).collect(Collectors.joining(", ")));
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                updatedTopic.setStatus(
                    Topic.TopicStatus.ofFailed("Error while updating topic configs: " + e.getMessage()));
                log.error(String.format("Error while updating topic configs %s on %s", key.name(),
                    managedClusterProperties.getName()), e);
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
                log.debug("Creating topic {} on {}", topic.getMetadata().getName(), topic.getMetadata().getCluster());
                NewTopic newTopic = new NewTopic(topic.getMetadata().getName(), topic.getSpec().getPartitions(),
                    (short) topic.getSpec().getReplicationFactor());
                newTopic.configs(topic.getSpec().getConfigs());
                return newTopic;
            })
            .toList();

        CreateTopicsResult createTopicsResult = getAdminClient().createTopics(newTopics);
        createTopicsResult.values().forEach((key, value) -> {
            Topic createdTopic = topics
                .stream()
                .filter(t -> t.getMetadata().getName().equals(key))
                .findFirst()
                .get();

            try {
                value.get(10, TimeUnit.SECONDS);
                createdTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                createdTopic.getMetadata().setGeneration(1);
                createdTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic created"));
                log.info("Success creating topic {} on {}", key, managedClusterProperties.getName());
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                createdTopic.setStatus(Topic.TopicStatus.ofFailed("Error while creating topic: " + e.getMessage()));
                log.error(
                    String.format("Error while creating topic %s on %s", key, managedClusterProperties.getName()),
                    e);
            }
            topicRepository.create(createdTopic);
        });
    }

    /**
     * Create tags and associate them.
     *
     * @param tagsToAssociate The tags to create and associate
     */
    private void createAndAssociateTags(List<TagTopicInfo> tagsToAssociate) {
        List<TagInfo> tagsToCreate = tagsToAssociate
            .stream()
            .map(tag -> TagInfo
                .builder()
                .name(tag.typeName())
                .build())
            .toList();

        String stringTags = String.join(", ", tagsToAssociate
            .stream()
            .map(Record::toString)
            .toList());

        schemaRegistryClient.createTags(tagsToCreate, managedClusterProperties.getName())
            .subscribe(
                successCreation ->
                    schemaRegistryClient.associateTags(
                            managedClusterProperties.getName(),
                            tagsToAssociate)
                        .subscribe(
                            successAssociation ->
                                log.info(String.format("Success associating tag %s.", stringTags)),
                            error ->
                                log.error(String.format("Error associating tag %s.", stringTags), error)),
                error -> log.error(String.format("Error creating tag %s.", stringTags), error));
    }

    /**
     * Dissociate tags to a topic.
     *
     * @param tagsToDissociate The tags to dissociate
     * @param topicName        The topic name
     */
    private void dissociateTags(Collection<String> tagsToDissociate, String topicName) {
        tagsToDissociate
            .forEach(tag -> schemaRegistryClient.dissociateTag(managedClusterProperties.getName(),
                    managedClusterProperties.getConfig().getProperty(CLUSTER_ID)
                        + ":" + topicName, tag)
                .subscribe(success -> log.info(String.format("Success dissociating tag %s.",
                        managedClusterProperties.getConfig().getProperty(CLUSTER_ID) + ":"
                            + topicName
                            + "/" + tag)),
                    error -> log.error(String.format("Error dissociating tag %s.",
                        managedClusterProperties.getConfig().getProperty(CLUSTER_ID) + ":"
                            + topicName
                            + "/" + tag), error)));
    }

    /**
     * Compute the configuration changes.
     *
     * @param expected The config from Ns4Kafka
     * @param actual   The config from cluster
     * @return A list of config
     */
    private Collection<AlterConfigOp> computeConfigChanges(Map<String, String> expected, Map<String, String> actual) {
        List<AlterConfigOp> toCreate = expected.entrySet()
            .stream()
            .filter(expectedEntry -> !actual.containsKey(expectedEntry.getKey()))
            .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(), expectedEntry.getValue()),
                AlterConfigOp.OpType.SET))
            .toList();

        List<AlterConfigOp> toDelete = actual.entrySet()
            .stream()
            .filter(actualEntry -> !expected.containsKey(actualEntry.getKey()))
            .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(), expectedEntry.getValue()),
                AlterConfigOp.OpType.DELETE))
            .toList();

        List<AlterConfigOp> toChange = expected.entrySet()
            .stream()
            .filter(expectedEntry -> {
                if (actual.containsKey(expectedEntry.getKey())) {
                    String actualVal = actual.get(expectedEntry.getKey());
                    String expectedVal = expectedEntry.getValue();
                    return !expectedVal.equals(actualVal);
                }
                return false;
            })
            .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(), expectedEntry.getValue()),
                AlterConfigOp.OpType.SET))
            .toList();

        List<AlterConfigOp> total = new ArrayList<>();
        total.addAll(toCreate);
        total.addAll(toDelete);
        total.addAll(toChange);

        return total;
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
    public Map<TopicPartition, RecordsToDelete> prepareRecordsToDelete(String topic)
        throws ExecutionException, InterruptedException {
        // List all partitions for topic and prepare a listOffsets call
        Map<TopicPartition, OffsetSpec> topicsPartitionsToDelete =
            getAdminClient().describeTopics(List.of(topic)).allTopicNames().get()
                .entrySet()
                .stream()
                .flatMap(topicDescriptionEntry -> topicDescriptionEntry.getValue().partitions().stream())
                .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                .collect(Collectors.toMap(Function.identity(), v -> OffsetSpec.latest()));

        // list all latest offsets for each partitions
        return getAdminClient().listOffsets(topicsPartitionsToDelete).all().get()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, kv -> RecordsToDelete.beforeOffset(kv.getValue().offset())));
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
        return getAdminClient().deleteRecords(recordsToDelete).lowWatermarks().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, kv -> {
                try {
                    var newValue = kv.getValue().get().lowWatermark();
                    log.info("Deleting records {} of topic-partition {}", newValue, kv.getKey());
                    return newValue;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error(String.format("Thread interrupted deleting records of topic-partition %s", kv.getKey()),
                        e);
                    return -1L;
                } catch (ExecutionException e) {
                    log.error(String.format("Execution error deleting records of topic-partition %s", kv.getKey()), e);
                    return -1L;
                } catch (Exception e) {
                    log.error(String.format("Error deleting records of topic-partition %s", kv.getKey()), e);
                    return -1L;
                }
            }));

    }
}
