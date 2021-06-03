package com.michelin.ns4kafka.services;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.executors.ConsumerGroupAsyncExecutor;

import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;

@Singleton
public class ConsumerGroupService {

    @Inject
    ApplicationContext applicationContext;

    @Inject
    AccessControlEntryService accessControlEntryService;

    public boolean isNamespaceOwnerOfConsumerGroup(String namespace, String groupId) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.GROUP, groupId);
    }

    public List<TopicPartition> getPartitionsToReset(Namespace namespace,String groupId, String topic) throws InterruptedException, ExecutionException {

        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
        Qualifiers.byName(namespace.getMetadata().getCluster()));
        //List<TopicPartition> result;
        //get partitions for a topic
        if (topic.equals("*")) {
            return consumerGroupAsyncExecutor.listConsumerGroupOffsets(groupId)
                .keySet().stream().collect(Collectors.toList());
        } else if (topic.contains(":")) {
            String[] splitResult = topic.split(":");
            return List.of(new TopicPartition(splitResult[0],Integer.parseInt(splitResult[1])));
        } else {
            return consumerGroupAsyncExecutor.listConsumerGroupOffsets(groupId)
                .keySet()
                .stream()
                .filter((topicPartition) -> topicPartition.topic().equals(topic))
                .collect(Collectors.toList());
        }
    }
    public Map<TopicPartition, Long> prepareOffsetsToReset(Namespace namespace, String groupId, String options, List<TopicPartition> partitionsToReset, ResetOffsetsMethod method) throws ParseException, InterruptedException, ExecutionException {

        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
        Qualifiers.byName(namespace.getMetadata().getCluster()));
        long timestamp;
        OffsetSpec offsetSpec = null;
        switch (method) {
            case SHIFT_BY:

                Map<TopicPartition,OffsetAndMetadata> currentCommittedOffset = consumerGroupAsyncExecutor.listConsumerGroupOffsets(groupId);
                int shiftValue = Integer.parseInt(options);
                Map<TopicPartition, Long> result = currentCommittedOffset.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset() + shiftValue));
                return result;
            case BY_DURATION:
                Duration duration = Duration.parse(options);
                timestamp = Instant.now().minus(duration).toEpochMilli();
                offsetSpec = OffsetSpec.forTimestamp(timestamp);
                break;
            case TO_DATETIME:
                OffsetDateTime dateTime = OffsetDateTime.parse(options);
                timestamp = dateTime.toInstant().toEpochMilli();
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
        return getNewOffset(consumerGroupAsyncExecutor, partitionsToReset, offsetSpec);

    }

    public Map<String, Long> apply(Namespace namespace, String consumerGroupId, Map<TopicPartition, Long> preparedOffsets) throws InterruptedException, ExecutionException {

        // set the new offset
        Map<String,Long> result = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> mapOffsetMetadata = preparedOffsets.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));

        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        consumerGroupAsyncExecutor.alterConsumerGroupOffsets(consumerGroupId, mapOffsetMetadata);
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
                // OffsetDateTime is of format iso6801 with time zone
                try {
                    OffsetDateTime.parse(options);
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

    private Map<TopicPartition,Long> getNewOffset(ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor,List<TopicPartition> partitionsToReset, OffsetSpec offsetSpec) throws InterruptedException, ExecutionException {

        //get the offset corresponding to the spec for each partition
        Map<TopicPartition, OffsetSpec> offsetsForTheSpec = new HashMap<>();
        partitionsToReset.stream().collect(Collectors.toMap(e->e, e -> offsetSpec));

        Map<TopicPartition, ListOffsetsResultInfo> newOffsets = consumerGroupAsyncExecutor.listOffsets(offsetsForTheSpec);
        Map<TopicPartition, Long> result = newOffsets.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        return result;
    }


}
