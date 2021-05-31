package com.michelin.ns4kafka.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.models.AccessControlEntry;
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

    @Inject
    AccessControlEntryService accessControlEntryService;

    public boolean isNamespaceOwnerOfConsumerGroup(String namespace, String consumerGroupName) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.GROUP, consumerGroupName);
    }

    public AlterConsumerGroupOffsetsResult resetOffset(Namespace namespace, String consumerGroupId, ConsumerGroupResetOffset consumerGroupResetOffset) throws InterruptedException, ExecutionException {

        AdminClient adminClient = generateAdminClientWithProperties(namespace);

        OffsetSpec offsetSpec = OffsetSpec.earliest();

        String topic = consumerGroupResetOffset.getSpec().getTopic();

        return setNewOffset(adminClient, consumerGroupId, topic, offsetSpec);
    }

    public AlterConsumerGroupOffsetsResult toTimeDateOffset(Namespace namespace, String consumerGroupId, ConsumerGroupResetOffset consumerGroupResetOffset) throws InterruptedException, ExecutionException {

        AdminClient adminClient = generateAdminClientWithProperties(namespace);

        OffsetSpec offsetSpec = OffsetSpec.forTimestamp(consumerGroupResetOffset.getSpec().getTimestamp());

        String topic = consumerGroupResetOffset.getSpec().getTopic();

        return setNewOffset(adminClient, consumerGroupId, topic, offsetSpec);
    }

    private AdminClient generateAdminClientWithProperties(Namespace namespace) {

        String cluster = namespace.getMetadata().getCluster();

        // get config
        Optional<KafkaAsyncExecutorConfig> configOptional = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(cluster))
                .findFirst();
        Properties config = configOptional.get().getConfig();

        return KafkaAdminClient.create(config);
    }

    private AlterConsumerGroupOffsetsResult setNewOffset(AdminClient adminClient, String consumerGroupId, String topic, OffsetSpec offsetSpec) throws InterruptedException, ExecutionException {


        //get partitions for a topic
        List<TopicPartitionInfo> partitions = adminClient.describeTopics(List.of(topic)).all().get().get(topic).partitions();

        //get the offset corresponding to the spec for each partition
        Map<TopicPartition, OffsetSpec> offsetsForTheSpec = new HashMap<>();
        partitions.forEach( partition -> {
                TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
                offsetsForTheSpec.put(topicPartition, offsetSpec);
        });
        Map<TopicPartition, ListOffsetsResultInfo> newOffsets = adminClient.listOffsets(offsetsForTheSpec).all().get();


        // set the new offset
        Map<TopicPartition, OffsetAndMetadata> mapOffsetMetadata = new HashMap<>();
        newOffsets.forEach( (topicPartition, offsetResultInfo) -> {
                mapOffsetMetadata.put(topicPartition, new OffsetAndMetadata(offsetResultInfo.offset()));
        });

        return adminClient.alterConsumerGroupOffsets(consumerGroupId, mapOffsetMetadata);
    }


}
