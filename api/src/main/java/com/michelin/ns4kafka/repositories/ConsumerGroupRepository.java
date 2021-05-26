package com.michelin.ns4kafka.repositories;

import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public interface ConsumerGroupRepository {
    void setOffset(String ConsumerGroupId, Map<TopicPartition, OffsetAndMetadata> offset);

}
