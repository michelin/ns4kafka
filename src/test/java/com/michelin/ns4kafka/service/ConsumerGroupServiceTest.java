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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroup;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetsSpec;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.service.executor.ConsumerGroupAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsumerGroupServiceTest {
    @Mock
    ApplicationContext applicationContext;

    @Mock
    AclService aclService;

    @Mock
    TopicService topicService;

    @Mock
    ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor;

    @InjectMocks
    ConsumerGroupService consumerGroupService;

    @Test
    void shouldListConsumerGroupsOwnedByNamespace() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("test")
                        .build())
                .build();

        ConsumerGroupDescription stableDescription =
                new ConsumerGroupDescription(null, true, null, null, null, GroupState.STABLE, null, null, null, null);

        TopicPartition partition = new TopicPartition("namespace-topic", 0);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.listConsumerGroupIds()).thenReturn(List.of("abc.group1", "def.group2"));
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "def.group2"))
                .thenReturn(false);
        when(consumerGroupAsyncExecutor.describeConsumerGroups(List.of("abc.group1")))
                .thenReturn(Map.of("abc.group1", stableDescription));
        when(consumerGroupAsyncExecutor.getCommittedOffsets("abc.group1")).thenReturn(Map.of(partition, 5L));
        when(consumerGroupAsyncExecutor.getLogEndOffsets(List.of(partition))).thenReturn(Map.of(partition, 12L));

        List<ConsumerGroup> result = consumerGroupService.findByWildcardName(namespace, "*");

        assertEquals(1, result.size());

        ConsumerGroup firstGroup = result.getFirst();
        assertEquals("abc.group1", firstGroup.getMetadata().getName());
        assertEquals("namespace", firstGroup.getMetadata().getNamespace());
        assertEquals("test", firstGroup.getMetadata().getCluster());
        assertEquals(GroupState.STABLE, firstGroup.getStatus().getState());
        assertEquals(1, firstGroup.getStatus().getOffsets().size());

        ConsumerGroup.ConsumerGroupOffset offset =
                firstGroup.getStatus().getOffsets().getFirst();
        assertEquals("namespace-topic", offset.getTopic());
        assertEquals(0, offset.getPartition());
        assertEquals(5L, offset.getCurrentOffset());
        assertEquals(12L, offset.getLogEndOffset());
        assertEquals(7L, offset.getLag());
    }

    @Test
    void shouldListConsumerGroupsOwnedByNamespaceWithoutOffsets() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("test")
                        .build())
                .build();

        ConsumerGroupDescription stableDescription =
                new ConsumerGroupDescription(null, true, null, null, null, GroupState.STABLE, null, null, null, null);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.listConsumerGroupIds()).thenReturn(List.of("abc.group1", "abc.group2"));
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group2"))
                .thenReturn(true);
        when(consumerGroupAsyncExecutor.describeConsumerGroups(List.of("abc.group1", "abc.group2")))
                .thenReturn(Map.of("abc.group1", stableDescription));

        List<ConsumerGroup> result = consumerGroupService.findByWildcardName(namespace, "*");

        assertEquals(2, result.size());

        ConsumerGroup firstGroup = result.getFirst();
        assertEquals("abc.group1", firstGroup.getMetadata().getName());
        assertEquals("namespace", firstGroup.getMetadata().getNamespace());
        assertEquals("test", firstGroup.getMetadata().getCluster());
        assertEquals(GroupState.STABLE, firstGroup.getStatus().getState());
        assertTrue(firstGroup.getStatus().getOffsets().isEmpty());

        ConsumerGroup secondGroup = result.get(1);
        assertEquals("abc.group2", secondGroup.getMetadata().getName());
        assertEquals(GroupState.UNKNOWN, secondGroup.getStatus().getState());
        assertTrue(secondGroup.getStatus().getOffsets().isEmpty());
        verify(consumerGroupAsyncExecutor, never()).getCommittedOffsets(anyString());
    }

    @Test
    void shouldListConsumerGroupsOwnedByNamespaceWithWildcardName() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("test")
                        .build())
                .build();

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.listConsumerGroupIds())
                .thenReturn(List.of("abc.group1", "abc.group2", "def.other-group"));
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group2"))
                .thenReturn(true);
        TopicPartition partition = new TopicPartition("topic2", 0);
        when(consumerGroupAsyncExecutor.describeConsumerGroups(List.of("abc.group2")))
                .thenReturn(Map.of());
        when(consumerGroupAsyncExecutor.getCommittedOffsets("abc.group2")).thenReturn(Map.of(partition, 3L));
        when(consumerGroupAsyncExecutor.getLogEndOffsets(List.of(partition))).thenReturn(Map.of(partition, 10L));

        List<ConsumerGroup> result = consumerGroupService.findByWildcardName(namespace, "*group2");

        assertEquals(1, result.size());
        assertEquals("abc.group2", result.getFirst().getMetadata().getName());
        assertEquals(1, result.getFirst().getStatus().getOffsets().size());

        ConsumerGroup.ConsumerGroupOffset offset =
                result.getFirst().getStatus().getOffsets().getFirst();
        assertEquals(3L, offset.getCurrentOffset());
        assertEquals(10L, offset.getLogEndOffset());
        assertEquals(7L, offset.getLag());
    }

    @Test
    void shouldListConsumerGroupsWithoutLogEndOffsets() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("test")
                        .build())
                .build();

        ConsumerGroupDescription stableDescription =
                new ConsumerGroupDescription(null, true, null, null, null, GroupState.STABLE, null, null, null, null);

        TopicPartition partition = new TopicPartition("namespace-topic", 0);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.listConsumerGroupIds()).thenReturn(List.of("abc.group1"));
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group1"))
                .thenReturn(true);
        when(consumerGroupAsyncExecutor.describeConsumerGroups(List.of("abc.group1")))
                .thenReturn(Map.of("abc.group1", stableDescription));
        when(consumerGroupAsyncExecutor.getCommittedOffsets("abc.group1")).thenReturn(Map.of(partition, 5L));
        when(consumerGroupAsyncExecutor.getLogEndOffsets(List.of(partition)))
                .thenThrow(new ExecutionException(new RuntimeException("boom")));

        List<ConsumerGroup> result = consumerGroupService.findByWildcardName(namespace, "*");

        assertEquals(1, result.size());

        ConsumerGroup.ConsumerGroupOffset offset =
                result.getFirst().getStatus().getOffsets().getFirst();
        assertEquals(5L, offset.getCurrentOffset());
        assertEquals(0L, offset.getLogEndOffset());
        assertEquals(0L, offset.getLag());
    }

    @Test
    void shouldListConsumerGroupsWithoutLogEndOffsetsWhenInterrupted() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("test")
                        .build())
                .build();

        ConsumerGroupDescription stableDescription =
                new ConsumerGroupDescription(null, true, null, null, null, GroupState.STABLE, null, null, null, null);

        TopicPartition partition = new TopicPartition("namespace-topic", 0);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.listConsumerGroupIds()).thenReturn(List.of("abc.group1"));
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group1"))
                .thenReturn(true);
        when(consumerGroupAsyncExecutor.describeConsumerGroups(List.of("abc.group1")))
                .thenReturn(Map.of("abc.group1", stableDescription));
        when(consumerGroupAsyncExecutor.getCommittedOffsets("abc.group1")).thenReturn(Map.of(partition, 5L));
        when(consumerGroupAsyncExecutor.getLogEndOffsets(List.of(partition)))
                .thenThrow(new InterruptedException("interrupted"));

        List<ConsumerGroup> result = consumerGroupService.findByWildcardName(namespace, "*");

        assertEquals(1, result.size());

        ConsumerGroup.ConsumerGroupOffset offset =
                result.getFirst().getStatus().getOffsets().getFirst();
        assertEquals(5L, offset.getCurrentOffset());
        assertEquals(0L, offset.getLogEndOffset());
        assertEquals(0L, offset.getLag());
        // The interrupt flag is set by the service; clear it so it does not leak to other tests
        assertTrue(Thread.interrupted());
    }

    @Test
    void shouldListConsumerGroupsWhenEmpty() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("test")
                        .build())
                .build();

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.listConsumerGroupIds()).thenReturn(List.of("abc.group1"));
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group1"))
                .thenReturn(false);

        assertEquals(List.of(), consumerGroupService.findByWildcardName(namespace, "*"));
    }

    @Test
    void shouldListExternalConsumerGroups() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("test")
                        .build())
                .build();

        ConsumerGroupDescription stableDescription =
                new ConsumerGroupDescription(null, true, null, null, null, GroupState.STABLE, null, null, null, null);

        TopicPartition ownedPartition = new TopicPartition("abc.namespace-topic", 0);
        TopicPartition ownedGroupForeignPartition = new TopicPartition("ghi.other-topic", 0);
        TopicPartition foreignPartition = new TopicPartition("def.other-topic", 0);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.listConsumerGroupIds())
                .thenReturn(List.of("abc.group1", "def.group1", "def.group2"));
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "def.group1"))
                .thenReturn(false);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "def.group2"))
                .thenReturn(false);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group1"))
                .thenReturn(true);
        when(consumerGroupAsyncExecutor.getCommittedOffsets("def.group1"))
                .thenReturn(Map.of(ownedPartition, 5L, ownedGroupForeignPartition, 9L));
        when(consumerGroupAsyncExecutor.getCommittedOffsets("def.group2")).thenReturn(Map.of(foreignPartition, 7L));
        when(topicService.isNamespaceOwnerOfTopic("namespace", "abc.namespace-topic"))
                .thenReturn(true);
        when(topicService.isNamespaceOwnerOfTopic("namespace", "ghi.other-topic"))
                .thenReturn(false);
        when(topicService.isNamespaceOwnerOfTopic("namespace", "def.other-topic"))
                .thenReturn(false);
        when(consumerGroupAsyncExecutor.describeConsumerGroups(List.of("def.group1")))
                .thenReturn(Map.of("def.group1", stableDescription));
        when(consumerGroupAsyncExecutor.getLogEndOffsets(List.of(ownedPartition)))
                .thenReturn(Map.of(ownedPartition, 8L));

        List<ConsumerGroup> result = consumerGroupService.findExternalByWildcardName(namespace, "*");

        assertEquals(1, result.size());
        assertEquals("def.group1", result.getFirst().getMetadata().getName());
        assertEquals(1, result.getFirst().getStatus().getOffsets().size());

        ConsumerGroup.ConsumerGroupOffset offset =
                result.getFirst().getStatus().getOffsets().getFirst();
        assertEquals("abc.namespace-topic", offset.getTopic());
        assertEquals(5L, offset.getCurrentOffset());
        assertEquals(8L, offset.getLogEndOffset());
        assertEquals(3L, offset.getLag());
    }

    @Test
    void shouldNotListExternalConsumerGroupsWhenCommittedOffsetsCannotBeRead()
            throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("namespace")
                        .cluster("test")
                        .build())
                .build();

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.listConsumerGroupIds()).thenReturn(List.of("abc.group1"));
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.GROUP, "abc.group1"))
                .thenReturn(false);
        when(consumerGroupAsyncExecutor.getCommittedOffsets("abc.group1"))
                .thenThrow(new ExecutionException(new RuntimeException("boom")));

        List<ConsumerGroup> result = consumerGroupService.findExternalByWildcardName(namespace, "*");

        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @CsvSource({"*", "namespace_testTopic01", "namespace_testTopic01:2"})
    void shouldValidateResetOnGivenTopics(String topic) {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic(topic)
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldValidateResetWhenMissingTopic() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic(":2")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @Test
    void shouldValidateResetToEarliest() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .options(null)
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldValidateResetToLatest() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.TO_LATEST)
                        .options(null)
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldValidateResetToDateTime() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.TO_DATETIME)
                        .options("2021-06-02T11:23:33.249+02:00")
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldValidateResetToDateTimeWithoutMs() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.TO_DATETIME)
                        .options("2021-06-02T11:22:33+02:00")
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @CsvSource({"NOT A DATE", "2021-06-02T11:22:33.249", "2021-06-02T11:22:33+99:99"})
    void shouldNotValidateResetWhenDateTimeOptionIsInvalid(String option) {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.TO_DATETIME)
                        .options(option)
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @ParameterizedTest
    @CsvSource({"-5", "+5"})
    void shouldValidateResetToShiftBy(String option) {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.SHIFT_BY)
                        .options(option)
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldNotValidateResetToShiftByWhenOptionIsInvalid() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.SHIFT_BY)
                        .options("Not an integer")
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @ParameterizedTest
    @CsvSource({"P4DT11H9M8S", "-P4DT11H9M8S"})
    void shouldValidateResetToDuration(String option) {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.BY_DURATION)
                        .options(option)
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldNotValidateResetToDurationWhenOptionIsInvalid() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.BY_DURATION)
                        .options("P4T11H9M8S")
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @Test
    void shouldNotValidateResetToOffsetWhenOptionHasWrongFormat() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.TO_OFFSET)
                        .options("not-integer")
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(
                "Invalid value \"not-integer\" for field \"to-offset\": value must be an integer.", result.getFirst());
    }

    @Test
    void shouldValidateResetToOffset() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("namespace_testTopic01:2")
                        .method(ResetOffsetsMethod.TO_OFFSET)
                        .options("-1")
                        .build())
                .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals("Invalid value \"-1\" for field \"to-offset\": value must be >= 0.", result.getFirst());
    }

    @Test
    void shouldGetPartitionsToResetFromAllTopics() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().cluster("test").build())
                .build();
        String groupId = "testGroup";
        String topic = "*";

        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        TopicPartition topicPartition3 = new TopicPartition("topic2", 0);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.getCommittedOffsets(groupId))
                .thenReturn(Map.of(
                        topicPartition1, 5L,
                        topicPartition2, 10L,
                        topicPartition3, 5L));
        List<TopicPartition> result = consumerGroupService.getPartitionsToReset(namespace, groupId, topic);

        assertEquals(3, result.size());

        List<TopicPartition> partitionsToReset = List.of(topicPartition1, topicPartition2, topicPartition3);
        assertEquals(new HashSet<>(partitionsToReset), new HashSet<>(result));
    }

    @Test
    void shouldGetPartitionsToResetFromOneTopic() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().cluster("test").build())
                .build();
        String groupId = "testGroup";
        String topic = "topic1";

        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.getTopicPartitions(topic))
                .thenReturn(List.of(topicPartition1, topicPartition2));
        List<TopicPartition> result = consumerGroupService.getPartitionsToReset(namespace, groupId, topic);

        assertEquals(2, result.size());
        assertEquals(new HashSet<>(List.of(topicPartition1, topicPartition2)), new HashSet<>(result));
    }

    @Test
    void shouldGetPartitionsToResetFromOneTopicPartition() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().cluster("test").build())
                .build();
        String groupId = "testGroup";
        String topic = "topic1:0";

        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);

        List<TopicPartition> result = consumerGroupService.getPartitionsToReset(namespace, groupId, topic);

        assertEquals(1, result.size());
        assertEquals(List.of(topicPartition1), result);
    }

    @Test
    void doPrepareOffsetsToResetShiftBy() throws ExecutionException, InterruptedException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().cluster("test").build())
                .build();

        String groupId = "testGroup";
        String options = "-5";
        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        List<TopicPartition> partitionsToReset = List.of(topicPartition1, topicPartition2);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.getCommittedOffsets(anyString()))
                .thenReturn(Map.of(
                        new TopicPartition("topic1", 0), 10L,
                        new TopicPartition("topic1", 1), 15L,
                        new TopicPartition("topic2", 0), 10L));
        when(consumerGroupAsyncExecutor.checkOffsetsRange(Map.of(
                        new TopicPartition("topic1", 0), 5L,
                        new TopicPartition("topic1", 1), 10L)))
                .thenReturn(Map.of(
                        new TopicPartition("topic1", 0), 5L,
                        new TopicPartition("topic1", 1), 10L));

        Map<TopicPartition, Long> result = consumerGroupService.prepareOffsetsToReset(
                namespace, groupId, options, partitionsToReset, ResetOffsetsMethod.SHIFT_BY);

        assertEquals(2, result.size());
        assertTrue(result.containsKey(topicPartition1));
        assertTrue(result.containsKey(topicPartition2));
        assertEquals(5, result.get(topicPartition1));
        assertEquals(10, result.get(topicPartition2));
    }

    @Test
    void shouldGetConsumerGroupStatusWhenDescribeCompletesNormally() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().cluster("test").build())
                .build();

        String groupId = "testGroup";
        ConsumerGroupDescription consumerGroupDescription =
                new ConsumerGroupDescription(null, true, null, null, null, GroupState.STABLE, null, null, null, null);

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.describeConsumerGroups(List.of(groupId)))
                .thenReturn(Map.of(groupId, consumerGroupDescription));

        GroupState result = consumerGroupService.getConsumerGroupStatus(namespace, groupId);

        assertEquals(GroupState.STABLE, result);
    }

    @Test
    void shouldGetConsumerGroupStatusWhenDescribeCompletesExceptionally()
            throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().cluster("test").build())
                .build();

        String groupId = "testGroup";

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.describeConsumerGroups(List.of(groupId)))
                .thenThrow(new ExecutionException("", new Exception()));

        GroupState result = consumerGroupService.getConsumerGroupStatus(namespace, groupId);

        assertEquals(GroupState.DEAD, result);
    }

    @Test
    void shouldDeleteConsumerGroup() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder().cluster("test").build())
                .build();
        String groupId = "testGroup";

        when(applicationContext.getBean(
                        ConsumerGroupAsyncExecutor.class,
                        Qualifiers.byName(namespace.getMetadata().getCluster())))
                .thenReturn(consumerGroupAsyncExecutor);

        consumerGroupService.deleteConsumerGroup(namespace, groupId);

        verify(consumerGroupAsyncExecutor).deleteConsumerGroups(List.of(groupId));
    }
}
