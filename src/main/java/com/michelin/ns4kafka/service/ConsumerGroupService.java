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
import static java.util.Comparator.comparing;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroup;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.service.executor.ConsumerGroupAsyncExecutor;
import com.michelin.ns4kafka.util.RegexUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;

/** Service to manage the consumer groups. */
@Singleton
public class ConsumerGroupService {
    private final ApplicationContext applicationContext;
    private final AclService aclService;
    private final TopicService topicService;

    /**
     * Constructor.
     *
     * @param applicationContext The application context
     * @param aclService The ACL service
     * @param topicService The topic service
     */
    public ConsumerGroupService(
            ApplicationContext applicationContext, AclService aclService, TopicService topicService) {
        this.applicationContext = applicationContext;
        this.aclService = aclService;
        this.topicService = topicService;
    }

    /**
     * Find all consumer groups owned by a given namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name filter
     * @return A list of consumer groups
     * @throws ExecutionException Any execution exception during consumer groups listing
     * @throws InterruptedException Any interrupted exception during consumer groups listing
     */
    public List<ConsumerGroup> findByWildcardName(Namespace namespace, String name)
            throws ExecutionException, InterruptedException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(
                ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        List<String> consumerGroupIds = consumerGroupAsyncExecutor.listConsumerGroupIds().stream()
                .filter(groupId ->
                        isNamespaceOwnerOfConsumerGroup(namespace.getMetadata().getName(), groupId))
                .filter(groupId -> RegexUtils.isResourceCoveredByRegex(groupId, nameFilterPatterns))
                .sorted()
                .toList();

        if (consumerGroupIds.isEmpty()) {
            return List.of();
        }

        Map<String, ConsumerGroupDescription> descriptions =
                consumerGroupAsyncExecutor.describeConsumerGroups(consumerGroupIds);

        boolean includeOffsets = consumerGroupIds.size() == 1;
        return consumerGroupIds.stream()
                .flatMap(groupId -> {
                    Map<TopicPartition, Long> committedOffsets =
                            includeOffsets ? getCommittedOffsets(consumerGroupAsyncExecutor, groupId) : Map.of();
                    return buildConsumerGroups(
                            namespace,
                            groupId,
                            descriptions.get(groupId),
                            committedOffsets,
                            getLogEndOffsets(consumerGroupAsyncExecutor, committedOffsets.keySet()))
                            .stream();
                })
                .toList();
    }

    /**
     * Find all external consumer groups consuming topics owned by a given namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name filter
     * @return A list of external consumer groups
     * @throws ExecutionException Any execution exception during consumer groups listing
     * @throws InterruptedException Any interrupted exception during consumer groups listing
     */
    public List<ConsumerGroup> findExternalByWildcardName(Namespace namespace, String name)
            throws ExecutionException, InterruptedException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(
                ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        Map<String, Map<TopicPartition, Long>> committedOffsetsByGroup =
                consumerGroupAsyncExecutor.listConsumerGroupIds().stream()
                        .filter(groupId -> RegexUtils.isResourceCoveredByRegex(groupId, nameFilterPatterns))
                        .filter(groupId -> !isNamespaceOwnerOfConsumerGroup(
                                namespace.getMetadata().getName(), groupId))
                        .collect(Collectors.toMap(
                                groupId -> groupId,
                                groupId -> getCommittedOffsets(consumerGroupAsyncExecutor, groupId).entrySet().stream()
                                        .filter(entry -> topicService.isNamespaceOwnerOfTopic(
                                                namespace.getMetadata().getName(),
                                                entry.getKey().topic()))
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));

        List<String> consumerGroupIds = committedOffsetsByGroup.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .sorted()
                .toList();

        if (consumerGroupIds.isEmpty()) {
            return List.of();
        }

        Map<String, ConsumerGroupDescription> descriptions =
                consumerGroupAsyncExecutor.describeConsumerGroups(consumerGroupIds);

        return consumerGroupIds.stream()
                .flatMap(groupId -> {
                    Map<TopicPartition, Long> committedOffsets = committedOffsetsByGroup.get(groupId);
                    return buildConsumerGroups(
                            namespace,
                            groupId,
                            descriptions.get(groupId),
                            committedOffsets,
                            getLogEndOffsets(consumerGroupAsyncExecutor, committedOffsets.keySet()))
                            .stream();
                })
                .toList();
    }

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
                } catch (NumberFormatException _) {
                    validationErrors.add(invalidConsumerGroupShiftBy(options));
                }
            }
            case BY_DURATION -> {
                try {
                    Duration.parse(options);
                } catch (NullPointerException | DateTimeParseException _) {
                    validationErrors.add(invalidConsumerGroupDuration(options));
                }
            }
            case TO_DATETIME -> {
                // OffsetDateTime is of format iso6801 with time zone
                try {
                    OffsetDateTime.parse(options);
                } catch (Exception _) {
                    validationErrors.add(invalidConsumerGroupDatetime(options));
                }
            }
            case TO_OFFSET -> {
                try {
                    int offset = Integer.parseInt(options);
                    if (offset < 0) {
                        validationErrors.add(invalidConsumerGroupOffsetNegative(options));
                    }
                } catch (NumberFormatException _) {
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
     * @throws InterruptedException Any interrupted exception during consumer groups description
     */
    public GroupState getConsumerGroupStatus(Namespace namespace, String groupId) throws InterruptedException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(
                ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        try {
            return consumerGroupAsyncExecutor
                    .describeConsumerGroups(List.of(groupId))
                    .get(groupId)
                    .groupState();
        } catch (ExecutionException _) {
            // If the consumer group doesn't exist, describeConsumerGroups throw an ExecutionException
            // Check KIP1043
            // https://cwiki.apache.org/confluence/display/KAFKA/KIP-1043%3A+Administration+of+groups#KIP1043:Administrationofgroups-Compatibility,Deprecation,andMigrationPlan
            return GroupState.DEAD;
        }
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
     * Delete a given consumer group.
     *
     * @param namespace The namespace
     * @param consumerGroupId The consumer group
     * @throws InterruptedException Any interrupted exception during consumer groups deletion
     * @throws ExecutionException Any execution exception during consumer groups deletion
     */
    public void deleteConsumerGroup(Namespace namespace, String consumerGroupId)
            throws InterruptedException, ExecutionException {
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = applicationContext.getBean(
                ConsumerGroupAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        consumerGroupAsyncExecutor.deleteConsumerGroups(List.of(consumerGroupId));
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

    /**
     * Build the flattened consumer group resources for a given group.
     *
     * @param namespace The namespace
     * @param consumerGroupId The consumer group
     * @param description The consumer group description
     * @param committedOffsets The committed offsets
     * @param logEndOffsets The log end offsets
     * @return The flattened consumer groups
     */
    private List<ConsumerGroup> buildConsumerGroups(
            Namespace namespace,
            String consumerGroupId,
            ConsumerGroupDescription description,
            Map<TopicPartition, Long> committedOffsets,
            Map<TopicPartition, Long> logEndOffsets) {
        GroupState state = description == null ? GroupState.UNKNOWN : description.groupState();

        if (committedOffsets.isEmpty()) {
            return List.of(buildConsumerGroup(
                    namespace,
                    consumerGroupId,
                    ConsumerGroup.ConsumerGroupStatus.builder().state(state).build()));
        }

        return committedOffsets.entrySet().stream()
                .sorted(comparing((Map.Entry<TopicPartition, Long> entry) ->
                                entry.getKey().topic())
                        .thenComparingInt(entry -> entry.getKey().partition()))
                .map(entry -> {
                    long currentOffset = entry.getValue();
                    Long logEndOffset = logEndOffsets.get(entry.getKey());
                    return buildConsumerGroup(
                            namespace,
                            consumerGroupId,
                            ConsumerGroup.ConsumerGroupStatus.builder()
                                    .state(state)
                                    .topic(entry.getKey().topic())
                                    .partition(entry.getKey().partition())
                                    .currentOffset(currentOffset)
                                    .logEndOffset(logEndOffset == null ? 0L : logEndOffset)
                                    .lag(logEndOffset == null ? 0L : logEndOffset - currentOffset)
                                    .build());
                })
                .toList();
    }

    /**
     * Build a consumer group resource with the given status.
     *
     * @param namespace The namespace
     * @param consumerGroupId The consumer group
     * @param status The consumer group status
     * @return The consumer group
     */
    private ConsumerGroup buildConsumerGroup(
            Namespace namespace, String consumerGroupId, ConsumerGroup.ConsumerGroupStatus status) {
        return ConsumerGroup.builder()
                .metadata(Resource.Metadata.builder()
                        .name(consumerGroupId)
                        .namespace(namespace.getMetadata().getName())
                        .cluster(namespace.getMetadata().getCluster())
                        .build())
                .status(status)
                .build();
    }

    /**
     * Get the committed offsets of a given consumer group.
     *
     * @param consumerGroupAsyncExecutor The consumer group async executor
     * @param groupId The consumer group
     * @return The committed offsets
     */
    private Map<TopicPartition, Long> getCommittedOffsets(
            ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor, String groupId) {
        try {
            return consumerGroupAsyncExecutor.getCommittedOffsets(groupId);
        } catch (ExecutionException _) {
            return Map.of();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return Map.of();
        }
    }

    /**
     * Get the log end offsets for a given set of topic-partitions.
     *
     * @param consumerGroupAsyncExecutor The consumer group async executor
     * @param partitions The topic-partitions
     * @return The log end offsets
     */
    private Map<TopicPartition, Long> getLogEndOffsets(
            ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor, Set<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return Map.of();
        }

        try {
            return consumerGroupAsyncExecutor.getLogEndOffsets(new ArrayList<>(partitions));
        } catch (ExecutionException _) {
            return Map.of();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return Map.of();
        }
    }
}
