package com.michelin.ns4kafka.services.executors;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.inject.Singleton;

import io.micronaut.context.annotation.EachBean;

@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class ConsumerGroupAsyncExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAsyncExecutorScheduler.class);
    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    public ConsumerGroupAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    public Void alterConsumerGroupOffsets(String consumerGroupId,
            Map<TopicPartition, OffsetAndMetadata> mapOffsetMetadata) throws InterruptedException, ExecutionException {

        return kafkaAsyncExecutorConfig.getAdminClient().alterConsumerGroupOffsets(consumerGroupId, mapOffsetMetadata).all().get();
    }

    public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String groupId) throws InterruptedException, ExecutionException {
        return kafkaAsyncExecutorConfig.getAdminClient().listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
    }

    public Map<TopicPartition, ListOffsetsResultInfo> listOffsets(Map<TopicPartition, OffsetSpec> offsetsForTheSpec) throws InterruptedException, ExecutionException {
        return kafkaAsyncExecutorConfig.getAdminClient().listOffsets(offsetsForTheSpec).all().get();
    }

}
