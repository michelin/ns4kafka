package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.TagSpecs;
import com.michelin.ns4kafka.services.clients.schema.entities.TagTopicInfo;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.michelin.ns4kafka.utils.config.ClusterConfig.CLUSTER_ID;
import static com.michelin.ns4kafka.utils.tags.TagsUtils.TOPIC_ENTITY_TYPE;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class TopicAsyncExecutor {
    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @Inject
    TopicRepository topicRepository;

    @Inject
    SchemaRegistryClient schemaRegistryClient;

    public TopicAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    private Admin getAdminClient(){
        return kafkaAsyncExecutorConfig.getAdminClient();
    }

    /**
     * Start topic synchronization
     */
    public void run() {
        if (this.kafkaAsyncExecutorConfig.isManageTopics()) {
            synchronizeTopics();
        }
    }

    /**
     * Start the synchronization of topics
     */
    public void synchronizeTopics() {
        log.debug("Starting topic collection for cluster {}", kafkaAsyncExecutorConfig.getName());

        try {
            Map<String, Topic> brokerTopics = collectBrokerTopics();
            List<Topic> ns4kafkaTopics = topicRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName());

            List<Topic> toCreate = ns4kafkaTopics.stream()
                    .filter(topic -> !brokerTopics.containsKey(topic.getMetadata().getName()))
                    .toList();

            List<Topic> toCheckConf = ns4kafkaTopics.stream()
                    .filter(topic -> brokerTopics.containsKey(topic.getMetadata().getName()))
                    .toList();

            Map<ConfigResource, Collection<AlterConfigOp>> toUpdate = toCheckConf.stream()
                    .map(topic -> {
                        Map<String,String> actualConf = brokerTopics.get(topic.getMetadata().getName()).getSpec().getConfigs();
                        Map<String,String> expectedConf = topic.getSpec().getConfigs() == null ? Map.of() : topic.getSpec().getConfigs();
                        Collection<AlterConfigOp> topicConfigChanges = computeConfigChanges(expectedConf,actualConf);
                        if (!topicConfigChanges.isEmpty()) {
                            ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topic.getMetadata().getName());
                            return Map.entry(cr,topicConfigChanges);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!toCreate.isEmpty()) {
                log.debug("Topic(s) to create: " +  String.join("," , toCreate.stream().map(topic -> topic.getMetadata().getName()).toList()));
            }

            if (!toUpdate.isEmpty()) {
                log.debug("Topic(s) to update: " + String.join("," , toUpdate.keySet().stream().map(ConfigResource::name).toList()));
                for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> e : toUpdate.entrySet()) {
                    for (AlterConfigOp op : e.getValue()) {
                        log.debug(e.getKey().name() + " " + op.opType().toString() + " " + op.configEntry().name() + "(" + op.configEntry().value() + ")");
                    }
                }
            }

            createTopics(toCreate);
            alterTopics(toUpdate, toCheckConf);

            createTags(ns4kafkaTopics, brokerTopics);
            deleteTags(ns4kafkaTopics, brokerTopics);

        } catch (ExecutionException | TimeoutException | CancellationException | KafkaStoreException e) {
            log.error("Error", e);
        } catch (InterruptedException e) {
            log.error("Error", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Create tags
     * @param ns4kafkaTopics Topics from ns4kafka
     * @param brokerTopics Topics from broker
     */
    public void createTags(List<Topic> ns4kafkaTopics, Map<String, Topic> brokerTopics) {
        List<TagSpecs> tagsToCreate = ns4kafkaTopics.stream().flatMap(ns4kafkaTopic -> {
            Topic brokerTopic = brokerTopics.get(ns4kafkaTopic.getMetadata().getName());

            List<String> existingTags = brokerTopic != null && brokerTopic.getMetadata().getTags() != null ? brokerTopic.getMetadata().getTags() : Collections.emptyList();
            List<String> newTags = ns4kafkaTopic.getMetadata().getTags() != null ? ns4kafkaTopic.getMetadata().getTags() : Collections.emptyList();

            return newTags.stream().filter(tag -> !existingTags.contains(tag)).map(tag -> TagSpecs.builder()
                    .entityName(kafkaAsyncExecutorConfig.getConfig().getProperty(CLUSTER_ID)+":"+ns4kafkaTopic.getMetadata().getName())
                    .typeName(tag)
                    .entityType(TOPIC_ENTITY_TYPE)
                    .build());
        }).toList();

        if(!tagsToCreate.isEmpty()) {
            schemaRegistryClient.addTags(kafkaAsyncExecutorConfig.getName(), tagsToCreate).block();
        }
    }

    /**
     * Delete tags
     * @param ns4kafkaTopics Topics from ns4kafka
     * @param brokerTopics Topics from broker
     */
    public void deleteTags(List<Topic> ns4kafkaTopics, Map<String, Topic> brokerTopics) {

        List<TagTopicInfo> tagsToDelete = brokerTopics.values().stream().flatMap(brokerTopic -> {
            Optional<Topic> newTopic = ns4kafkaTopics.stream()
                    .filter(ns4kafkaTopic -> ns4kafkaTopic.getMetadata().getName().equals(brokerTopic.getMetadata().getName()))
                    .findFirst();
            List<String> newTags = newTopic.isPresent() && newTopic.get().getMetadata().getTags() != null ? newTopic.get().getMetadata().getTags() : Collections.emptyList();
            List<String> existingTags = brokerTopic.getMetadata().getTags() != null ? brokerTopic.getMetadata().getTags() : Collections.emptyList();

            return existingTags.stream().filter(tag -> !newTags.contains(tag)).map(tag -> TagTopicInfo.builder()
                    .entityName(kafkaAsyncExecutorConfig.getConfig().getProperty(CLUSTER_ID)+":"+brokerTopic.getMetadata().getName())
                    .typeName(tag)
                    .entityType(TOPIC_ENTITY_TYPE)
                    .build());
        }).toList();

        tagsToDelete.forEach(tag -> schemaRegistryClient.deleteTag(kafkaAsyncExecutorConfig.getName(), tag.entityName(), tag.typeName()).block());
    }

    /**
     * Delete a topic
     * @param topic The topic to delete
     */
    public void deleteTopic(Topic topic) throws InterruptedException, ExecutionException, TimeoutException {
        getAdminClient().deleteTopics(List.of(topic.getMetadata().getName())).all().get(30, TimeUnit.SECONDS);
        log.info("Success deleting topic {} on {}", topic.getMetadata().getName(), this.kafkaAsyncExecutorConfig.getName());
    }

    /**
     * Collect all topics on broker
     * @return All topics by name
     */
    public Map<String, Topic> collectBrokerTopics() throws ExecutionException, InterruptedException, TimeoutException {
        return collectBrokerTopicsFromNames(listBrokerTopicNames());
    }

    /**
     * List all topic names on broker
     * @return All topic names
     */
    public List<String> listBrokerTopicNames() throws InterruptedException, ExecutionException, TimeoutException {
        return getAdminClient().listTopics().listings()
                .get(30, TimeUnit.SECONDS)
                .stream()
                .map(TopicListing::name)
                .toList();
    }

    public Map<String, Topic> collectBrokerTopicsFromNames(List<String> topicNames) throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, TopicDescription> topicDescriptions = getAdminClient().describeTopics(topicNames).all().get();

        // Create a Map<TopicName, Map<ConfigName, ConfigValue>> for all topics
        // includes only Dynamic config properties
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
                                        .filter( configEntry -> configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))
                ))
                .entrySet()
                .stream()
                .map(stringMapEntry -> Topic.builder()
                        .metadata(ObjectMeta.builder()
                                .cluster(kafkaAsyncExecutorConfig.getName())
                                .name(stringMapEntry.getKey())
                                .build())
                        .spec(Topic.TopicSpec.builder()
                                .replicationFactor(topicDescriptions.get(stringMapEntry.getKey()).partitions().get(0).replicas().size())
                                .partitions(topicDescriptions.get(stringMapEntry.getKey()).partitions().size())
                                .configs(stringMapEntry.getValue())
                                .build())
                        .build()
                )
                .collect(Collectors.toMap( topic -> topic.getMetadata().getName(), Function.identity()));

        completeWithTags(topics);

        return topics;
    }

    /**
     * Complete topics with confluent tags
     * @param topics Topics to complete
     */
    public void completeWithTags(Map<String, Topic> topics) {
        if(kafkaAsyncExecutorConfig.getProvider().equals(KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD)) {
            topics.entrySet().stream()
                    .forEach(entry ->
                            entry.getValue().getMetadata().setTags(schemaRegistryClient.getTopicWithTags(kafkaAsyncExecutorConfig.getName(),
                                            kafkaAsyncExecutorConfig.getConfig().getProperty(CLUSTER_ID) + ":" + entry.getValue().getMetadata().getName())
                                    .block().stream().map(TagTopicInfo::typeName).toList()));
        }
    }

    private void alterTopics(Map<ConfigResource, Collection<AlterConfigOp>> toUpdate, List<Topic> topics) {
        AlterConfigsResult alterConfigsResult = getAdminClient().incrementalAlterConfigs(toUpdate);
        alterConfigsResult.values().forEach((key, value) -> {
            Topic updatedTopic = topics.stream().filter(t -> t.getMetadata().getName().equals(key.name())).findFirst().get();
            try {
                value.get(10, TimeUnit.SECONDS);
                Collection<AlterConfigOp> ops = toUpdate.get(key);
                updatedTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                updatedTopic.getMetadata().setGeneration(updatedTopic.getMetadata().getGeneration() + 1);
                updatedTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic configs updated"));
                log.info("Success updating topic configs {} on {}: [{}]",
                        key.name(),
                        kafkaAsyncExecutorConfig.getName(),
                        ops.stream().map(AlterConfigOp::toString).collect(Collectors.joining(",")));
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                updatedTopic.setStatus(Topic.TopicStatus.ofFailed("Error while updating topic configs: " + e.getMessage()));
                log.error(String.format("Error while updating topic configs %s on %s", key.name(), this.kafkaAsyncExecutorConfig.getName()), e);
            }
            topicRepository.create(updatedTopic);
        });
    }

    private void createTopics(List<Topic> topics) {
        List<NewTopic> newTopics = topics.stream()
                .map(topic -> {
                    log.debug("Creating topic {} on {}",topic.getMetadata().getName(),topic.getMetadata().getCluster());
                    NewTopic newTopic = new NewTopic(topic.getMetadata().getName(),topic.getSpec().getPartitions(), (short) topic.getSpec().getReplicationFactor());
                    newTopic.configs(topic.getSpec().getConfigs());
                    log.debug("{}",newTopic);
                    return newTopic;
                })
                .toList();

        CreateTopicsResult createTopicsResult = getAdminClient().createTopics(newTopics);
        createTopicsResult.values().forEach((key, value) -> {
            Topic createdTopic = topics.stream().filter(t -> t.getMetadata().getName().equals(key)).findFirst().get();
            try {
                value.get(10, TimeUnit.SECONDS);
                createdTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                createdTopic.getMetadata().setGeneration(1);
                createdTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic created"));
                log.info("Success creating topic {} on {}", key, this.kafkaAsyncExecutorConfig.getName());
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                createdTopic.setStatus(Topic.TopicStatus.ofFailed("Error while creating topic: " + e.getMessage()));
                log.error(String.format("Error while creating topic %s on %s", key, this.kafkaAsyncExecutorConfig.getName()), e);
            }
            topicRepository.create(createdTopic);
        });
    }

    private Collection<AlterConfigOp> computeConfigChanges(Map<String,String> expected, Map<String,String> actual){
        List<AlterConfigOp> toCreate = expected.entrySet()
                .stream()
                .filter(expectedEntry -> !actual.containsKey(expectedEntry.getKey()))
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),expectedEntry.getValue()), AlterConfigOp.OpType.SET))
                .toList();

        List<AlterConfigOp> toDelete = actual.entrySet()
                .stream()
                .filter(actualEntry -> !expected.containsKey(actualEntry.getKey()))
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),expectedEntry.getValue()), AlterConfigOp.OpType.DELETE))
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
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),expectedEntry.getValue()), AlterConfigOp.OpType.SET))
                .toList();

        List<AlterConfigOp> total = new ArrayList<>();
        total.addAll(toCreate);
        total.addAll(toDelete);
        total.addAll(toChange);

        return total;
    }

    /**
     * For a given topic, get each latest offset by partition in order to delete all the records
     * before these offsets
     * @param topic The topic to delete records
     * @return A map of offsets by topic-partitions
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, RecordsToDelete> prepareRecordsToDelete(String topic) throws ExecutionException, InterruptedException {
        // List all partitions for topic and prepare a listOffsets call
        Map<TopicPartition, OffsetSpec> topicsPartitionsToDelete = getAdminClient().describeTopics(List.of(topic)).all().get()
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
     * Delete the records for each partition, before each offset
     * @param recordsToDelete The offsets by topic-partitions
     * @return The new offsets by topic-partitions
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, Long> deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete) throws InterruptedException {
        return getAdminClient().deleteRecords(recordsToDelete).lowWatermarks().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, kv-> {
                    try {
                        var newValue = kv.getValue().get().lowWatermark();
                        log.info("Deleting records {} of topic-partition {}", newValue, kv.getKey());
                        return newValue;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error(String.format("Thread interrupted deleting records of topic-partition %s", kv.getKey()), e);
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
