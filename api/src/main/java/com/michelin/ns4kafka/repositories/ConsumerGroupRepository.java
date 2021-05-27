package com.michelin.ns4kafka.repositories;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public interface ConsumerGroupRepository {
    AlterConsumerGroupOffsetsResult setOffset(Properties config, String ConsumerGroupId, Map<TopicPartition, OffsetAndMetadata> offsets);

}
