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
package com.michelin.ns4kafka.service.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.property.ManagedClusterProperties;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsumerGroupAsyncExecutorTest {
    @Mock
    ManagedClusterProperties managedClusterProperties;

    @Mock
    Admin adminClient;

    @Mock
    ListGroupsResult listGroupsResult;

    @InjectMocks
    ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor;

    @Test
    void shouldListOnlyConsumerGroups() throws ExecutionException, InterruptedException {
        GroupListing classicConsumerGroup = new GroupListing(
                "classic-consumer-group", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE));
        GroupListing newConsumerGroup = new GroupListing(
                "new-consumer-group", Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE));
        GroupListing connectGroup = new GroupListing(
                "connect-group", Optional.of(GroupType.CLASSIC), "connect", Optional.of(GroupState.STABLE));
        GroupListing streamsGroup = new GroupListing(
                "streams-group", Optional.of(GroupType.STREAMS), "stream", Optional.of(GroupState.STABLE));
        GroupListing shareGroup =
                new GroupListing("share-group", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE));
        GroupListing emptyProtocolGroup =
                new GroupListing("empty-protocol-group", Optional.empty(), "", Optional.empty());

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.listGroups()).thenReturn(listGroupsResult);
        when(listGroupsResult.all())
                .thenReturn(KafkaFuture.completedFuture(List.of(
                        classicConsumerGroup,
                        newConsumerGroup,
                        connectGroup,
                        streamsGroup,
                        shareGroup,
                        emptyProtocolGroup)));

        List<String> result = consumerGroupAsyncExecutor.listConsumerGroupIds();

        assertEquals(3, result.size());
        assertTrue(result.contains("classic-consumer-group"));
        assertTrue(result.contains("new-consumer-group"));
        assertTrue(result.contains("empty-protocol-group"));
    }

    @Test
    void shouldFilterOutConnectGroups() throws ExecutionException, InterruptedException {
        GroupListing connectGroup = new GroupListing(
                "compose-connect-group", Optional.of(GroupType.CLASSIC), "connect", Optional.of(GroupState.STABLE));
        GroupListing consumerGroup = new GroupListing(
                "my-consumer-group", Optional.of(GroupType.CLASSIC), "consumer", Optional.of(GroupState.STABLE));

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.listGroups()).thenReturn(listGroupsResult);
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(connectGroup, consumerGroup)));

        List<String> result = consumerGroupAsyncExecutor.listConsumerGroupIds();

        assertEquals(1, result.size());
        assertEquals("my-consumer-group", result.getFirst());
    }

    @Test
    void shouldReturnEmptyListWhenNoConsumerGroups() throws ExecutionException, InterruptedException {
        GroupListing streamsGroup = new GroupListing(
                "streams-group", Optional.of(GroupType.STREAMS), "stream", Optional.of(GroupState.STABLE));

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.listGroups()).thenReturn(listGroupsResult);
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(streamsGroup)));

        List<String> result = consumerGroupAsyncExecutor.listConsumerGroupIds();

        assertTrue(result.isEmpty());
    }
}
