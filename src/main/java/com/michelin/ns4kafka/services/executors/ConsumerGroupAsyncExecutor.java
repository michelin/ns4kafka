package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class ConsumerGroupAsyncExecutor {
    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    public ConsumerGroupAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    /**
     * Getter for Kafka Admin client
     * @return A Kafka Admin client instance
     */
    private Admin getAdminClient() {
        return kafkaAsyncExecutorConfig.getAdminClient();
    }

    public Map<String, ConsumerGroupDescription> describeConsumerGroups(List<String> groupIds) throws ExecutionException, InterruptedException {
        return getAdminClient().describeConsumerGroups(groupIds).all().get();
    }

    public void alterConsumerGroupOffsets(String consumerGroupId, Map<TopicPartition, Long> preparedOffsets)
            throws InterruptedException, ExecutionException {
        getAdminClient().alterConsumerGroupOffsets(consumerGroupId,
                preparedOffsets.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())))
        ).all().get();
        log.info("Consumer Group {} changed offset", consumerGroupId);
        if (log.isDebugEnabled()) {
            preparedOffsets.forEach((topicPartition, offset)-> log.debug("TopicPartition {} has the new offset {}", topicPartition, offset));
        }
    }

    /**
     * Find offsets matching the offset specs for given partition (e.g.: last offset for latest spec)
     * @param offsetsForTheSpec The offset specs
     * @return A map of topic-partition and offsets
     * @throws ExecutionException Any execution exception during offsets description
     * @throws InterruptedException Any interrupted exception during offsets description
     */
    public Map<TopicPartition, Long> listOffsets(Map<TopicPartition, OffsetSpec> offsetsForTheSpec) throws InterruptedException, ExecutionException {
        return getAdminClient().listOffsets(offsetsForTheSpec)
                .all()
                .get()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, kv -> kv.getValue().offset()));
    }

    /**
     * Get all the committed offsets of a given consumer group
     * @param groupId The consumer group
     * @return A map of topic-partition and committed offset number
     * @throws ExecutionException Any execution exception during consumer groups description
     * @throws InterruptedException Any interrupted exception during consumer groups description
     */
    public Map<TopicPartition, Long> getCommittedOffsets(String groupId) throws ExecutionException, InterruptedException {
        return getAdminClient().listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    /**
     * Get the list of partitions of a given topic
     * @param topicName The topic name
     * @return A list of partitions
     * @throws ExecutionException Any execution exception during topics description
     * @throws InterruptedException Any interrupted exception during topics description
     */
    public List<TopicPartition> getTopicPartitions(String topicName) throws ExecutionException, InterruptedException {
        return getAdminClient().describeTopics(Collections.singletonList(topicName))
                .all()
                .get()
                .get(topicName)
                .partitions()
                .stream()
                .map(partitionInfo -> new TopicPartition(topicName, partitionInfo.partition()))
                .toList();
    }

    /**
     * Get earliest offsets for given list of topic-partitions
     * @param partitionsToReset The topic-partitions list
     * @return A map of topic-partition and offsets
     * @throws ExecutionException Any execution exception during offsets description
     * @throws InterruptedException Any interrupted exception during offsets description
     */
    public Map<TopicPartition, Long> getLogStartOffsets(List<TopicPartition> partitionsToReset) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> startOffsets = partitionsToReset
                .stream()
                .collect(Collectors.toMap(Function.identity(), v -> OffsetSpec.earliest()));
        return listOffsets(startOffsets);
    }

    /**
     * Get latest offsets for given list of topic-partitions
     * @param partitionsToReset The topic-partitions list
     * @return A map of topic-partition and offsets
     * @throws ExecutionException Any execution exception during offsets description
     * @throws InterruptedException Any interrupted exception during offsets description
     */
    public Map<TopicPartition, Long> getLogEndOffsets(List<TopicPartition> partitionsToReset) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> endOffsets = partitionsToReset
                .stream()
                .collect(Collectors.toMap(Function.identity(), v -> OffsetSpec.latest()));
        return listOffsets(endOffsets);
    }

    /**
     * Get offsets from timestamp for given list of topic-partitions
     * @param partitionsToReset The topic-partitions list
     * @param timestamp The timestamp used to filter
     * @return A map of topic-partition and offsets
     * @throws ExecutionException Any execution exception during offsets description
     * @throws InterruptedException Any interrupted exception during offsets description
     */
    public Map<TopicPartition, Long> getLogTimestampOffsets(List<TopicPartition> partitionsToReset, long timestamp) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> dateOffsets = partitionsToReset
                .stream()
                .collect(Collectors.toMap(Function.identity(), e -> OffsetSpec.forTimestamp(timestamp)));
        // list offsets for this timestamp
        Map<TopicPartition, Long> offsets = listOffsets(dateOffsets);

        // extract successful offsets (>= 0)
        Map<TopicPartition, Long> successfulLogTimestampOffsets = offsets.entrySet().stream()
                .filter(e -> e.getValue() >= 0)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // extract failed offsets partitions (== -1)
        List<TopicPartition> unsuccessfulPartitions = offsets.entrySet().stream()
                .filter(e -> e.getValue() == -1L)
                .map(Map.Entry::getKey)
                .toList();

        // reprocess failed offsets to OffsetSpec.latest()
        Map<TopicPartition, Long> reprocessedUnsuccessfulOffsets = getLogEndOffsets(unsuccessfulPartitions);
        successfulLogTimestampOffsets.putAll(reprocessedUnsuccessfulOffsets);

        return successfulLogTimestampOffsets;
    }

    /**
     * Check if given offsets for topic-partitions are properly between earliest and latest offsets
     * @param requestedOffsets The offsets to check for topic-partitions
     * @return A map of topic-partition and offsets with no unbound offsets
     * @throws ExecutionException Any execution exception during offsets description
     * @throws InterruptedException Any interrupted exception during offsets description
     */
    public Map<TopicPartition, Long> checkOffsetsRange(Map<TopicPartition, Long> requestedOffsets) throws ExecutionException, InterruptedException {
        // lower bound
        Map<TopicPartition, Long> logStartOffsets = getLogStartOffsets(new ArrayList<>(requestedOffsets.keySet()));
        // upper bound
        Map<TopicPartition, Long> logEndOffsets = getLogEndOffsets(new ArrayList<>(requestedOffsets.keySet()));

        // replace inside boundaries if required
        return requestedOffsets.entrySet().stream()
                .map(entry -> {
                    if (entry.getValue() > logEndOffsets.get(entry.getKey())) {
                        // went too much forward
                        return Map.entry(entry.getKey(), logEndOffsets.get(entry.getKey()));
                    } else if (entry.getValue() < logStartOffsets.get(entry.getKey())) {
                        // went too much backward
                        return Map.entry(entry.getKey(), logStartOffsets.get(entry.getKey()));
                    } else {
                        return entry;
                    }
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
