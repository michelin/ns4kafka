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
package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetsSpec;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsetsResponse;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.ConsumerGroupService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.security.utils.SecurityService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsumerGroupControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    ConsumerGroupService consumerGroupService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    ConsumerGroupController consumerGroupController;

    @Test
    void shouldResetConsumerGroup() throws InterruptedException, ExecutionException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(Metadata.builder().name("groupID").cluster("local").build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .options(null)
                        .build())
                .build();

        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        List<TopicPartition> topicPartitions = List.of(topicPartition1, topicPartition2);

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset)).thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);
        when(consumerGroupService.getConsumerGroupStatus(ns, "groupID")).thenReturn(GroupState.EMPTY);
        when(consumerGroupService.getPartitionsToReset(ns, "groupID", "topic1")).thenReturn(topicPartitions);
        when(consumerGroupService.prepareOffsetsToReset(
                        ns, "groupID", null, topicPartitions, ResetOffsetsMethod.TO_EARLIEST))
                .thenReturn(Map.of(topicPartition1, 5L, topicPartition2, 10L));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        List<ConsumerGroupResetOffsetsResponse> result =
                consumerGroupController.resetOffsets("test", "groupID", resetOffset, false);

        ConsumerGroupResetOffsetsResponse response = result.stream()
                .filter(topicPartitionOffset -> topicPartitionOffset.getSpec().getPartition() == 0)
                .findFirst()
                .orElse(null);

        assertNotNull(response);
        assertEquals(5L, response.getSpec().getOffset());

        ConsumerGroupResetOffsetsResponse response2 = result.stream()
                .filter(topicPartitionOffset -> topicPartitionOffset.getSpec().getPartition() == 1)
                .findFirst()
                .orElse(null);

        assertNotNull(response2);
        assertEquals(10L, response2.getSpec().getOffset());

        verify(consumerGroupService)
                .alterConsumerGroupOffsets(ArgumentMatchers.eq(ns), ArgumentMatchers.eq("groupID"), anyMap());
    }

    @Test
    void shouldResetConsumerGroupInDryRunMode() throws InterruptedException, ExecutionException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(Metadata.builder().name("groupID").cluster("local").build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        List<TopicPartition> topicPartitions = List.of(topicPartition1, topicPartition2);

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset)).thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);
        when(consumerGroupService.getConsumerGroupStatus(ns, "groupID")).thenReturn(GroupState.EMPTY);
        when(consumerGroupService.getPartitionsToReset(ns, "groupID", "topic1")).thenReturn(topicPartitions);
        when(consumerGroupService.prepareOffsetsToReset(
                        ns, "groupID", null, topicPartitions, ResetOffsetsMethod.TO_EARLIEST))
                .thenReturn(Map.of(topicPartition1, 5L, topicPartition2, 10L));

        List<ConsumerGroupResetOffsetsResponse> result =
                consumerGroupController.resetOffsets("test", "groupID", resetOffset, true);

        ConsumerGroupResetOffsetsResponse resultTopicPartition1 = result.stream()
                .filter(topicPartitionOffset -> topicPartitionOffset.getSpec().getPartition() == 0)
                .findFirst()
                .orElse(null);

        assertNotNull(resultTopicPartition1);
        assertEquals(5L, resultTopicPartition1.getSpec().getOffset());

        ConsumerGroupResetOffsetsResponse resultTopicPartition2 = result.stream()
                .filter(topicPartitionOffset -> topicPartitionOffset.getSpec().getPartition() == 1)
                .findFirst()
                .orElse(null);

        assertNotNull(resultTopicPartition2);
        assertEquals(10L, resultTopicPartition2.getSpec().getOffset());

        verify(consumerGroupService, never()).alterConsumerGroupOffsets(notNull(), anyString(), anyMap());
    }

    @Test
    void shouldHandleExceptionDuringReset() throws InterruptedException, ExecutionException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(Metadata.builder().name("groupID").cluster("local").build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset)).thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);
        when(consumerGroupService.getConsumerGroupStatus(ns, "groupID")).thenReturn(GroupState.EMPTY);
        when(consumerGroupService.getPartitionsToReset(ns, "groupID", "topic1"))
                .thenThrow(new ExecutionException("Error during getPartitionsToReset", new Throwable()));

        ExecutionException result = assertThrows(
                ExecutionException.class,
                () -> consumerGroupController.resetOffsets("test", "groupID", resetOffset, false));

        assertEquals("Error during getPartitionsToReset", result.getMessage());
    }

    @Test
    void shouldNotResetConsumerGroupWhenNotOwner() {
        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(Metadata.builder().name("groupID").cluster("local").build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        when(consumerGroupService.validateResetOffsets(resetOffset)).thenReturn(new ArrayList<>());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(false);

        ResourceValidationException result = assertThrows(
                ResourceValidationException.class,
                () -> consumerGroupController.resetOffsets("test", "groupID", resetOffset, false));

        assertLinesMatch(
                List.of("Invalid value \"groupID\" for field \"group\": " + "namespace is not owner of the resource."),
                result.getValidationErrors());
    }

    @Test
    void shouldNotResetConsumerGroupWhenValidationErrors() {
        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(Metadata.builder().name("groupID").cluster("local").build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        when(consumerGroupService.validateResetOffsets(resetOffset)).thenReturn(List.of("Validation Error"));
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);

        ResourceValidationException result = assertThrows(
                ResourceValidationException.class,
                () -> consumerGroupController.resetOffsets("test", "groupID", resetOffset, false));

        assertLinesMatch(List.of("Validation Error"), result.getValidationErrors());
    }

    @Test
    void shouldNotResetConsumerGroupWhenItIsStable() throws InterruptedException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(Metadata.builder().name("groupID").cluster("local").build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset)).thenReturn(new ArrayList<>());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);
        when(consumerGroupService.getConsumerGroupStatus(ns, "groupID")).thenReturn(GroupState.STABLE);

        ResourceValidationException result = assertThrows(
                ResourceValidationException.class,
                () -> consumerGroupController.resetOffsets("test", "groupID", resetOffset, false));

        assertEquals(
                "Invalid \"reset offset\" operation: offsets can only be reset if the consumer group "
                        + "\"groupID\" is " + GroupState.EMPTY.toString().toLowerCase() + " but the current state is "
                        + GroupState.STABLE.toString().toLowerCase()
                        + ". Stop the consumption and wait \"session.timeout.ms\" before retrying.",
                result.getValidationErrors().getFirst());
    }
}
