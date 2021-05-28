package com.michelin.ns4kafka.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffset;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

@Singleton
public class ConsumerGroupService {

    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    public AlterConsumerGroupOffsetsResult resetOffset(Namespace namespace, String consumerGroupId, ConsumerGroupResetOffset consumerGroupResetOffset) throws InterruptedException, ExecutionException {

        String cluster = namespace.getMetadata().getCluster();

        // get config
        Optional<KafkaAsyncExecutorConfig> configOptional = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(cluster))
                .findFirst();
        Properties config = configOptional.get().getConfig();

        Map<TopicPartition, OffsetAndMetadata> mapOffset = new HashMap<>();
        String topic = consumerGroupResetOffset.getSpec().getTopic();

        AdminClient adminClient = KafkaAdminClient.create(config);
        List<TopicPartitionInfo> partitions = adminClient.describeTopics(List.of(topic)).all().get().get(topic).partitions();

        Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();

        //get the earliest offset for each partition
        partitions.forEach( partition -> {
                TopicPartition topicPartition = new TopicPartition(consumerGroupResetOffset.getSpec().getTopic(), partition.partition());
                earliest.put(topicPartition, OffsetSpec.earliest());
        });

        Map<TopicPartition, ListOffsetsResultInfo> earliestOffsets = adminClient.listOffsets(earliest).all().get();

        // set the new offset
        earliestOffsets.forEach( (topicPartition, offsetResultInfo) -> {
                //TODO Change to another method to find the earliest offset of a topic
                mapOffset.put(topicPartition, new OffsetAndMetadata(0));
        });

        return adminClient.alterConsumerGroupOffsets(consumerGroupId, mapOffset);
    }

}
