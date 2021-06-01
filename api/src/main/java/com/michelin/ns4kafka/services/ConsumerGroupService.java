package com.michelin.ns4kafka.services;

import java.sql.Time;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import com.nimbusds.jose.crypto.impl.MACProvider;

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

    public boolean isNamespaceOwnerOfConsumerGroup(String namespace, String groupId) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.GROUP, groupId);
    }

    public List<TopicPartition> getPartitionsToReset(Namespace namespace,String groupId, String topic) throws InterruptedException, ExecutionException {

        AdminClient adminClient = generateAdminClientWithProperties(namespace);
        //List<TopicPartition> result;
        //get partitions for a topic
        if (topic.equals("*")) {
            return adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
                .keySet().stream().collect(Collectors.toList());
        } else if (topic.contains(":")) {
            String[] splitResult = topic.split(":");
            return List.of(new TopicPartition(splitResult[0],Integer.parseInt(splitResult[1])));
        } else {
            return adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
                .keySet()
                .stream()
                .filter((topicPartition) -> topicPartition.topic().equals(topic))
                .collect(Collectors.toList());
        }
    }
    public Map<TopicPartition, Long> prepareOffsetsToReset(Namespace namespace, String groupId, String options, List<TopicPartition> partitionsToReset, ResetOffsetsMethod method) throws ParseException, InterruptedException, ExecutionException {

        AdminClient adminClient = generateAdminClientWithProperties(namespace);
        long timestamp;
        OffsetSpec offsetSpec = null;
        switch (method) {
            case SHIFT_BY:
                Map<TopicPartition,OffsetAndMetadata> currentCommittedOffset = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
                int shiftValue = Integer.parseInt(options);
                Map<TopicPartition, Long> result = new HashMap<>();
                partitionsToReset.forEach((partitionToReset) -> {
                        result.put(partitionToReset, currentCommittedOffset.get(partitionToReset).offset() + shiftValue);
                    });
                return result;
            case BY_DURATION:
                Duration duration = Duration.parse(options);
                timestamp = Instant.now().minus(duration).toEpochMilli();
                offsetSpec = OffsetSpec.forTimestamp(timestamp);
                break;
            case TO_DATETIME:
                DateFormat iso8601DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                timestamp = iso8601DateFormat.parse(options).getTime();
                offsetSpec = OffsetSpec.forTimestamp(timestamp);
                break;
            case TO_LATEST:
                offsetSpec = OffsetSpec.latest();
                break;
            case TO_EARLIEST:
                offsetSpec = OffsetSpec.earliest();
                break;
            default:
                break;
        }
        return getNewOffset(adminClient, partitionsToReset, offsetSpec);

    }

    public Map<String, Long> apply(Namespace namespace, String consumerGroupId, Map<TopicPartition, Long> preparedOffsets) throws InterruptedException, ExecutionException {

        AdminClient adminClient = generateAdminClientWithProperties(namespace);
        // set the new offset
        Map<String,Long> result = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> mapOffsetMetadata = new HashMap<>();
        preparedOffsets.forEach((topicPartition, offset) -> {
            mapOffsetMetadata.put(topicPartition, new OffsetAndMetadata(offset));
            result.put(topicPartition.toString(), offset);
        });

        adminClient.alterConsumerGroupOffsets(consumerGroupId, mapOffsetMetadata).all().get();
        return result;
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

    private AdminClient generateAdminClientWithProperties(Namespace namespace) {

        String cluster = namespace.getMetadata().getCluster();

        // get config
        Optional<KafkaAsyncExecutorConfig> configOptional = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(cluster))
                .findFirst();
        Properties config = configOptional.get().getConfig();

        return KafkaAdminClient.create(config);
    }

    private Map<TopicPartition,Long> getNewOffset(AdminClient adminClient,  List<TopicPartition> partitionsToReset, OffsetSpec offsetSpec) throws InterruptedException, ExecutionException {

        //get the offset corresponding to the spec for each partition
        Map<TopicPartition, OffsetSpec> offsetsForTheSpec = new HashMap<>();
        partitionsToReset.forEach(topicPartition -> {
                offsetsForTheSpec.put(topicPartition, offsetSpec);
            });

        Map<TopicPartition, ListOffsetsResultInfo> newOffsets = adminClient.listOffsets(offsetsForTheSpec).all().get();
        Map<TopicPartition, Long> result = new HashMap<>();
        newOffsets.forEach((topicPartition, listOffsetResultInfo) -> {
                result.put(topicPartition, listOffsetResultInfo.offset());
        });
        return result;
    }


}
