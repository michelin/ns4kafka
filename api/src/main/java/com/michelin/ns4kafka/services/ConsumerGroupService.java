package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.repositories.ConsumerGroupRepository;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Singleton
public class ConsumerGroupService {

    @Inject
    ConsumerGroupRepository consumerGroupRepository;

    public void toOffset(String consumerGroupId, Map<TopicPartition, OffsetAndMetadata> map) {

        consumerGroupRepository.setOffset(consumerGroupId, map);
    }

}
