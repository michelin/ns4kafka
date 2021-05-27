
package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.repositories.ConsumerGroupRepository;

import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Singleton
public class KafkaConsumerGroupRepository  implements ConsumerGroupRepository {


	@Override
	public AlterConsumerGroupOffsetsResult setOffset(Properties config, String ConsumerGroupId, Map<TopicPartition, OffsetAndMetadata> offsets) {
		AdminClient adminClient = KafkaAdminClient.create(config);
		return adminClient.alterConsumerGroupOffsets(ConsumerGroupId, offsets);
	}

}
