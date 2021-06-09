package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.executors.ConsumerGroupAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Singleton
public class ConsumerGroupService {

    @Inject
    ApplicationContext applicationContext;

    @Inject
    AccessControlEntryService accessControlEntryService;

    public boolean isNamespaceOwnerOfConsumerGroup(String namespace, String groupId) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.GROUP, groupId);
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

    public String getConsumerGroupStatus(Namespace namespace, String groupId) throws ExecutionException, InterruptedException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        return consumerGroupAsyncExecutor.describeConsumerGroups(List.of(groupId)).get(groupId).state().toString();
    }

    public List<TopicPartition> getPartitionsToReset(Namespace namespace, String groupId, String topic) throws InterruptedException, ExecutionException {

        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        //List<TopicPartition> result;
        //get partitions for a topic
        if (topic.equals("*")) {
            return new ArrayList<>(consumerGroupAsyncExecutor.getCommittedOffsets(groupId).keySet());
        } else if (topic.contains(":")) {
            String[] splitResult = topic.split(":");
            return List.of(new TopicPartition(splitResult[0], Integer.parseInt(splitResult[1])));
        } else {
            return consumerGroupAsyncExecutor.getCommittedOffsets(groupId)
                    .keySet()
                    .stream()
                    .filter((topicPartition) -> topicPartition.topic().equals(topic))
                    .collect(Collectors.toList());
        }
    }

    public Map<TopicPartition, Long> prepareOffsetsToReset(Namespace namespace, String groupId, String options, List<TopicPartition> partitionsToReset, ResetOffsetsMethod method) throws InterruptedException, ExecutionException {

        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        switch (method) {
            case SHIFT_BY:
                int shiftBy = Integer.parseInt(options);
                Map<TopicPartition, Long> currentCommittedOffsets = consumerGroupAsyncExecutor.getCommittedOffsets(groupId);
                Map<TopicPartition, Long> requestedOffsets = partitionsToReset.stream()
                        .map(e -> {
                            if (currentCommittedOffsets.containsKey(e)) {
                                return Map.entry(e, currentCommittedOffsets.get(e) + shiftBy);
                            }
                            throw new IllegalArgumentException("Cannot shift offset for partition " + e.toString() + " since there is no current committed offset");
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return consumerGroupAsyncExecutor.checkOffsetsRange(groupId, requestedOffsets);
            case BY_DURATION:
                Duration duration = Duration.parse(options);
                return consumerGroupAsyncExecutor.getLogTimestampOffsets(groupId, partitionsToReset, Instant.now().minus(duration).toEpochMilli());
            case TO_DATETIME:
                OffsetDateTime dateTime = OffsetDateTime.parse(options);
                return consumerGroupAsyncExecutor.getLogTimestampOffsets(groupId, partitionsToReset, dateTime.toInstant().toEpochMilli());
            case TO_LATEST:
                return consumerGroupAsyncExecutor.getLogEndOffsets(groupId, partitionsToReset);
            case TO_EARLIEST:
                return consumerGroupAsyncExecutor.getLogStartOffsets(groupId, partitionsToReset);
            default:
                throw new IllegalArgumentException("Unreachable code");
        }
    }

    public void alterConsumerGroupOffsets(Namespace namespace, String consumerGroupId, Map<TopicPartition, Long> preparedOffsets) throws InterruptedException, ExecutionException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        consumerGroupAsyncExecutor.alterConsumerGroupOffsets(consumerGroupId, preparedOffsets);

    }
}
