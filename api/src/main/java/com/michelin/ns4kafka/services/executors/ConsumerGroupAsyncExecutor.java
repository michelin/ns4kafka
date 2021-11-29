package com.michelin.ns4kafka.services.executors;

import io.micronaut.context.annotation.EachBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import jakarta.inject.Singleton;
import java.util.ArrayList;
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

    public Map<TopicPartition, Long> listOffsets(Map<TopicPartition, OffsetSpec> offsetsForTheSpec)
            throws InterruptedException, ExecutionException {
        return getAdminClient().listOffsets(offsetsForTheSpec)
                .all()
                .get()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, kv -> kv.getValue().offset()));
    }

    public Map<TopicPartition, Long> getCommittedOffsets(String groupId) throws ExecutionException, InterruptedException {
        return getAdminClient().listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    public Map<TopicPartition, Long> getLogStartOffsets(String groupId, List<TopicPartition> partitionsToReset) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> startOffsets = partitionsToReset
                .stream()
                .collect(Collectors.toMap(Function.identity(), v -> OffsetSpec.earliest()));
        return listOffsets(startOffsets);
    }

    public Map<TopicPartition, Long> getLogEndOffsets(String groupId, List<TopicPartition> partitionsToReset) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> endOffsets = partitionsToReset
                .stream()
                .collect(Collectors.toMap(Function.identity(), v -> OffsetSpec.latest()));
        return listOffsets(endOffsets);
    }

    public Map<TopicPartition, Long> getLogTimestampOffsets(String groupId, List<TopicPartition> partitionsToReset, long timestamp) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> dateOffsets = partitionsToReset
                .stream()
                .collect(Collectors.toMap(Function.identity(), e -> OffsetSpec.forTimestamp(timestamp)));
        // list offsets for this timestamp
        Map<TopicPartition, Long> offsets = listOffsets(dateOffsets);

        // extract successful offsets (>= 0)
        Map<TopicPartition, Long> successfulLogTimestampOffsets = offsets.entrySet().stream()
                .filter(e -> e.getValue() >= 0)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // extract failed offsets paritions (== -1)
        List<TopicPartition> unsuccessfulPartitions = offsets.entrySet().stream()
                .filter(e -> e.getValue() == -1L)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        // reprocess failed offsets to OffsetSpec.latest()
        Map<TopicPartition, Long> reprocessedUnsuccessfulOffsets = getLogEndOffsets(groupId, unsuccessfulPartitions);
        successfulLogTimestampOffsets.putAll(reprocessedUnsuccessfulOffsets);

        return successfulLogTimestampOffsets;
    }

    public Map<TopicPartition, Long> checkOffsetsRange(String groupId, Map<TopicPartition, Long> requestedOffsets) throws ExecutionException, InterruptedException {
        // lower bound
        Map<TopicPartition, Long> logStartOffsets = getLogStartOffsets(groupId, new ArrayList<>(requestedOffsets.keySet()));
        // upper bound
        Map<TopicPartition, Long> logEndOffsets = getLogEndOffsets(groupId, new ArrayList<>(requestedOffsets.keySet()));

        // replace inside boundaries if required
        return requestedOffsets.entrySet().stream()
                .map(entry -> {
                    if (entry.getValue() > logEndOffsets.get(entry.getKey())) {
                        // went too much foward
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