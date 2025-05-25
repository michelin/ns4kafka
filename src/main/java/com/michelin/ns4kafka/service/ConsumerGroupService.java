/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConsumerGroupDatetime;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConsumerGroupDuration;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConsumerGroupOffsetInteger;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConsumerGroupOffsetNegative;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConsumerGroupShiftBy;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConsumerGroupTopic;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.service.executor.ConsumerGroupAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
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
import org.apache.kafka.common.TopicPartition;

/** Service to manage the consumer groups. */
@Singleton
public class ConsumerGroupService {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private AclService aclService;

    /**
     * Check if a given namespace is owner of a given group.
     *
     * @param namespace The namespace
     * @param groupId The group
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfConsumerGroup(String namespace, String groupId) {
        return aclService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.GROUP, groupId);
    }

    /**
     * Validate the given reset offsets options.
     *
     * @param consumerGroupResetOffsets The reset offsets options
     * @return A list of validation errors
     */
    public List<String> validateResetOffsets(ConsumerGroupResetOffsets consumerGroupResetOffsets) {
        List<String> validationErrors = new ArrayList<>();

        // validate topic
        // allowed : *, <topic>, <topic:partition>
        Pattern validTopicValue = Pattern.compile("^(\\*|[a-zA-Z0-9-_.]+(:[0-9]+)?)$");
        if (!validTopicValue
                .matcher(consumerGroupResetOffsets.getSpec().getTopic())
                .matches()) {
            validationErrors.add(invalidConsumerGroupTopic(
                    consumerGroupResetOffsets.getSpec().getTopic()));
        }

        String options = consumerGroupResetOffsets.getSpec().getOptions();

        switch (consumerGroupResetOffsets.getSpec().getMethod()) {
            case SHIFT_BY -> {
                try {
                    Integer.parseInt(options);
                } catch (NumberFormatException e) {
                    validationErrors.add(invalidConsumerGroupShiftBy(options));
                }
            }
            case BY_DURATION -> {
                try {
                    Duration.parse(options);
                } catch (NullPointerException | DateTimeParseException e) {
                    validationErrors.add(invalidConsumerGroupDuration(options));
                }
            }
            case TO_DATETIME -> {
                // OffsetDateTime is of format iso6801 with time zone
                try {
                    OffsetDateTime.parse(options);
                } catch (Exception e) {
                    validationErrors.add(invalidConsumerGroupDatetime(options));
                }
            }
            case TO_OFFSET -> {
                try {
                    int offset = Integer.parseInt(options);
                    if (offset < 0) {
                        validationErrors.add(invalidConsumerGroupOffsetNegative(options));
                    }
                } catch (NumberFormatException e) {
                    validationErrors.add(invalidConsumerGroupOffsetInteger(options));
                }
            }
            default -> {
                // Nothing to do
            }
        }
        return validationErrors;
    }

    /**
     * Get the status of a given consumer group.
     *
     * @param namespace The namespace
     * @param groupId The consumer group
     * @return The consumer group status
     * @throws ExecutionException Any execution exception during consumer groups description
     * @throws InterruptedException Any interrupted exception during consumer groups description
     */
    public String getConsumerGroupStatus(Namespace namespace, String groupId)
            throws ExecutionException, InterruptedException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(
                ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        return consumerGroupAsyncExecutor
                .describeConsumerGroups(List.of(groupId))
                .get(groupId)
                .state()
                .toString();
    }

    /**
     * Get the partitions of a topic to reset.
     *
     * @param namespace The namespace
     * @param groupId The consumer group of the topic to reset
     * @param topic the topic to reset
     * @return A list of topic partitions
     * @throws ExecutionException Any execution exception during consumer groups description
     * @throws InterruptedException Any interrupted exception during consumer groups description
     */
    public List<TopicPartition> getPartitionsToReset(Namespace namespace, String groupId, String topic)
            throws InterruptedException, ExecutionException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(
                ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        if (topic.equals("*")) {
            return new ArrayList<>(
                    consumerGroupAsyncExecutor.getCommittedOffsets(groupId).keySet());
        } else if (topic.contains(":")) {
            String[] splitResult = topic.split(":");
            return List.of(new TopicPartition(splitResult[0], Integer.parseInt(splitResult[1])));
        } else {
            return consumerGroupAsyncExecutor.getTopicPartitions(topic);
        }
    }

    /**
     * From given options, compute the new offsets for given topic-partitions.
     *
     * @param namespace The namespace
     * @param groupId The consumer group
     * @param options Given additional options for offsets reset
     * @param partitionsToReset The list of partitions to reset
     * @param method The method of offsets reset
     * @return A map with new offsets for topic-partitions
     * @throws ExecutionException Any execution exception during consumer groups description
     * @throws InterruptedException Any interrupted exception during consumer groups description
     */
    public Map<TopicPartition, Long> prepareOffsetsToReset(
            Namespace namespace,
            String groupId,
            String options,
            List<TopicPartition> partitionsToReset,
            ResetOffsetsMethod method)
            throws InterruptedException, ExecutionException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(
                ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        switch (method) {
            case SHIFT_BY -> {
                int shiftBy = Integer.parseInt(options);
                Map<TopicPartition, Long> currentCommittedOffsets =
                        consumerGroupAsyncExecutor.getCommittedOffsets(groupId);
                Map<TopicPartition, Long> requestedOffsets = partitionsToReset.stream()
                        .map(topicPartition -> {
                            if (currentCommittedOffsets.containsKey(topicPartition)) {
                                return Map.entry(topicPartition, currentCommittedOffsets.get(topicPartition) + shiftBy);
                            }
                            throw new IllegalArgumentException("Cannot shift offset for partition "
                                    + topicPartition.toString() + " since there is no current committed offset");
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return consumerGroupAsyncExecutor.checkOffsetsRange(requestedOffsets);
            }
            case BY_DURATION -> {
                Duration duration = Duration.parse(options);
                return consumerGroupAsyncExecutor.getLogTimestampOffsets(
                        partitionsToReset, Instant.now().minus(duration).toEpochMilli());
            }
            case TO_DATETIME -> {
                OffsetDateTime dateTime = OffsetDateTime.parse(options);
                return consumerGroupAsyncExecutor.getLogTimestampOffsets(
                        partitionsToReset, dateTime.toInstant().toEpochMilli());
            }
            case TO_LATEST -> {
                return consumerGroupAsyncExecutor.getLogEndOffsets(partitionsToReset);
            }
            case TO_EARLIEST -> {
                return consumerGroupAsyncExecutor.getLogStartOffsets(partitionsToReset);
            }
            case TO_OFFSET -> {
                Map<TopicPartition, Long> toRequestedOffset = partitionsToReset.stream()
                        .map(topicPartition -> Map.entry(topicPartition, Long.parseLong(options)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return consumerGroupAsyncExecutor.checkOffsetsRange(toRequestedOffset);
            }
            default -> throw new IllegalArgumentException("Unreachable code");
        }
    }

    /**
     * Alter the offsets of a given consumer group.
     *
     * @param namespace The namespace
     * @param consumerGroupId The consumer group
     * @param preparedOffsets The new offsets
     * @throws InterruptedException Any interrupted exception during offsets alteration
     * @throws ExecutionException Any execution exception during offsets alteration
     */
    public void alterConsumerGroupOffsets(
            Namespace namespace, String consumerGroupId, Map<TopicPartition, Long> preparedOffsets)
            throws InterruptedException, ExecutionException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(
                ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        consumerGroupAsyncExecutor.alterConsumerGroupOffsets(consumerGroupId, preparedOffsets);
    }
}
