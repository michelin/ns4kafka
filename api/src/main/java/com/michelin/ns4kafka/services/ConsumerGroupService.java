package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.repositories.ConsumerGroupRepository;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;


import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Singleton
public class ConsumerGroupService {

    @Inject
    ConsumerGroupRepository consumerGroupRepository;

    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    public AlterConsumerGroupOffsetsResult setOffset(String cluster,String consumerGroupId, Map<TopicPartition, OffsetAndMetadata> map) {
        Optional<KafkaAsyncExecutorConfig> configOptional = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(cluster))
                .findFirst();
        Properties config = configOptional.get().getConfig();

        return consumerGroupRepository.setOffset(config, consumerGroupId, map);
    }

}
