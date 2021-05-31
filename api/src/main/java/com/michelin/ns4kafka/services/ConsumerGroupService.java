package com.michelin.ns4kafka.services;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;

import io.micronaut.core.util.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Singleton
public class ConsumerGroupService {

    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Inject
    AccessControlEntryService accessControlEntryService;

    public boolean isNamespaceOwnerOfConsumerGroup(String namespace, String consumerGroupName) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.GROUP, consumerGroupName);
    }

    public resultWithOffsets resetOffset(Namespace namespace, String consumerGroupId, ConsumerGroupResetOffsets consumerGroupResetOffsets) throws InterruptedException, ExecutionException {

        AdminClient adminClient = generateAdminClientWithProperties(namespace);

        OffsetSpec offsetSpec = OffsetSpec.earliest();

        String topic = consumerGroupResetOffsets.getSpec().getTopic();

        return setNewOffset(adminClient, consumerGroupId, topic, offsetSpec);
    }

    public resultWithOffsets toTimeDateOffset(Namespace namespace, String consumerGroupId, ConsumerGroupResetOffsets consumerGroupResetOffsets) throws InterruptedException, ExecutionException {

        AdminClient adminClient = generateAdminClientWithProperties(namespace);

        OffsetSpec offsetSpec = OffsetSpec.forTimestamp(consumerGroupResetOffsets.getSpec().getTimestamp());

        String topic = consumerGroupResetOffsets.getSpec().getTopic();

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

    private resultWithOffsets setNewOffset(AdminClient adminClient, String consumerGroupId, String topic, OffsetSpec offsetSpec) throws InterruptedException, ExecutionException {


        //get partitions for a topic
        List<TopicPartitionInfo> partitions = adminClient.describeTopics(List.of(topic)).all().get().get(topic).partitions();

        //get the offset corresponding to the spec for each partition
        Map<TopicPartition, OffsetSpec> offsetsForTheSpec = new HashMap<>();
        partitions.forEach(partition -> {
            TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
            offsetsForTheSpec.put(topicPartition, offsetSpec);
        });
        Map<TopicPartition, ListOffsetsResultInfo> newOffsets = adminClient.listOffsets(offsetsForTheSpec).all().get();


        // set the new offset
        Map<TopicPartition, OffsetAndMetadata> mapOffsetMetadata = new HashMap<>();
        newOffsets.forEach((topicPartition, offsetResultInfo) -> {
            mapOffsetMetadata.put(topicPartition, new OffsetAndMetadata(offsetResultInfo.offset()));
            new OffsetAndMetadata(0).offset();
        });

        AlterConsumerGroupOffsetsResult result = adminClient.alterConsumerGroupOffsets(consumerGroupId, mapOffsetMetadata);
        return new resultWithOffsets(result, mapOffsetMetadata);
    }

    public List<String> validateResetOffsets(ConsumerGroupResetOffsets consumerGroupResetOffsets) {
        List<String> validationErrors = new ArrayList<>();
        // validate topic
        // allowed : *, <topic>, <topic:partition>
        Pattern validTopicValue = Pattern.compile("^(\\*|[a-zA-Z0-9-_.]+(:[0-9]+)?)$");
        if (!validTopicValue.matcher(consumerGroupResetOffsets.getSpec().getTopic()).matches()) {
            validationErrors.add("Invalid value " + consumerGroupResetOffsets.getSpec().getTopic() + " for topic : Value must match" +
                    " [*, <topic>, <topic:partition>]");
        }

        String options = consumerGroupResetOffsets.getSpec().getOptions();

        switch (consumerGroupResetOffsets.getSpec().getMethod()) {
            case SHIFT_BY:
                try {
                    Integer.parseInt(options);
                } catch (NumberFormatException e) {
                    validationErrors.add("Invalid value " + options + " for options : Value must be an Integer");
                }
                break;
            case BY_DURATION:
                try {
                    Duration.parse(options);
                } catch (NullPointerException | DateTimeParseException e) {
                    validationErrors.add("Invalid value " + options + " for options : Value must be an ISO 8601 Duration [ PnDTnHnMnS ]");
                }
                break;
            case TO_DATETIME:
                DateFormat iso8601DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                try {
                    iso8601DateFormat.parse(options);
                } catch (Exception e) {
                    validationErrors.add("Invalid value " + options + " for options : Value must be an ISO 8601 DateTime with Time zone [ yyyy-MM-dd'T'HH:mm:ss.SSSXXX ]");
                }
                break;
            case TO_LATEST:
            case TO_EARLIEST:
            default:
                // Nothing to do
                break;
        }
        return validationErrors;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class resultWithOffsets {
        private AlterConsumerGroupOffsetsResult result;
        private Map<TopicPartition, OffsetAndMetadata> mapOffset;
    }

}
