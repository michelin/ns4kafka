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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.*;
import com.michelin.ns4kafka.repository.StreamRepository;
import com.michelin.ns4kafka.service.executor.AccessControlEntryAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamServiceTest {
    @InjectMocks
    StreamService streamService;

    @Mock
    AclService aclService;

    @Mock
    StreamRepository streamRepository;

    @Mock
    TopicService topicService;

    @Mock
    ApplicationContext applicationContext;

    @Mock
    AccessControlEntryAsyncExecutor aceAsyncExecutor;

    @Test
    void shouldFindAllForClusterWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(streamRepository.findAllForCluster("local")).thenReturn(List.of());

        var actual = streamService.findAllForNamespace(ns);
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldFindAllForCluster() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();
        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream1")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();
        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream2")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();
        KafkaStream stream3 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream3")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        when(streamRepository.findAllForCluster("local")).thenReturn(List.of(stream1, stream2, stream3));

        var actual = streamService.findAllForNamespace(ns);

        assertEquals(3, actual.size());
        assertTrue(actual.contains(stream1));
        assertTrue(actual.contains(stream2));
        assertTrue(actual.contains(stream3));
    }

    @Test
    void shouldFindAllForNamespaceWithParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream1")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream2")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream3 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream3")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        when(streamRepository.findAllForCluster("local")).thenReturn(List.of(stream1, stream2, stream3));

        List<KafkaStream> list1 = streamService.findByWildcardName(ns, "test_stream3");
        assertEquals(List.of(stream3), list1);

        List<KafkaStream> list2 = streamService.findByWildcardName(ns, "test_stream5");
        assertTrue(list2.isEmpty());

        List<KafkaStream> list3 = streamService.findByWildcardName(ns, "");
        assertEquals(List.of(stream1, stream2, stream3), list3);
    }

    @Test
    void shouldFindAllForNamespaceWithWildcardParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream1")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream2")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream3 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream3")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream4 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test.stream1")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream5 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("stream2_test")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream6 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("prefix.stream_test1")
                        .namespace("test2")
                        .cluster("local")
                        .build())
                .build();

        when(streamRepository.findAllForCluster("local"))
                .thenReturn(List.of(stream1, stream2, stream3, stream4, stream5, stream6));

        assertEquals(List.of(stream1, stream2, stream3), streamService.findByWildcardName(ns, "test_*"));
        assertEquals(List.of(stream1, stream2, stream3), streamService.findByWildcardName(ns, "test_stream?"));
        assertEquals(List.of(stream1, stream2, stream3, stream5), streamService.findByWildcardName(ns, "*_*"));
        assertEquals(List.of(stream1, stream4), streamService.findByWildcardName(ns, "test?stream1"));
        assertEquals(List.of(stream2, stream5), streamService.findByWildcardName(ns, "*stream2*"));
        assertTrue(streamService.findByWildcardName(ns, "*stream5").isEmpty());
        assertTrue(streamService.findByWildcardName(ns, "test??stream1").isEmpty());
        assertTrue(streamService.findByWildcardName(ns, ".*").isEmpty());
        assertEquals(List.of(stream1, stream2, stream3, stream4, stream5), streamService.findByWildcardName(ns, "*"));
    }

    @Test
    void shouldFindByName() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();
        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream1")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();
        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream2")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();
        KafkaStream stream3 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream3")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        when(streamRepository.findAllForCluster("local")).thenReturn(List.of(stream1, stream2, stream3));

        var actual = streamService.findByName(ns, "test_stream2");

        assertTrue(actual.isPresent());
        assertEquals(stream2, actual.get());
    }

    @Test
    void shouldFindByNameWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(streamRepository.findAllForCluster("local")).thenReturn(List.of());

        var actual = streamService.findByName(ns, "test_stream2");

        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldNamespaceBeOwnerOfStreams() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        AccessControlEntry ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("test.")
                        .grantedTo("test")
                        .build())
                .build();

        AccessControlEntry ace2 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.GROUP)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("test.")
                        .grantedTo("test")
                        .build())
                .build();

        AccessControlEntry ace3 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("test.")
                        .grantedTo("test")
                        .build())
                .build();

        AccessControlEntry ace4 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("test-bis.")
                        .grantedTo("test")
                        .build())
                .build();

        AccessControlEntry ace5 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.GROUP)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("test-bis.")
                        .grantedTo("test")
                        .build())
                .build();

        AccessControlEntry ace6 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.GROUP)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("test-ter.")
                        .grantedTo("test")
                        .build())
                .build();

        AccessControlEntry ace7 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("test-qua.")
                        .grantedTo("test")
                        .build())
                .build();

        when(aclService.findAllGrantedToNamespace(ns)).thenReturn(List.of(ace1, ace2, ace3, ace4, ace5, ace6, ace7));

        assertTrue(streamService.isNamespaceOwnerOfKafkaStream(ns, "test.stream"));
        assertFalse(streamService.isNamespaceOwnerOfKafkaStream(ns, "test-bis.stream"), "ACL are LITERAL");
        assertFalse(streamService.isNamespaceOwnerOfKafkaStream(ns, "test-ter.stream"), "Topic ACL missing");
        assertFalse(streamService.isNamespaceOwnerOfKafkaStream(ns, "test-qua.stream"), "Group ACL missing");
        assertFalse(streamService.isNamespaceOwnerOfKafkaStream(ns, "test-nop.stream"), "No ACL");
    }

    @Test
    void shouldDeleteKafkaStreamAndRelatedTopics() throws Exception {

        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder().name("ns").cluster("local").build())
                .build();
        KafkaStream stream = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("stream")
                        .namespace("ns")
                        .cluster("local")
                        .build())
                .build();
        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder().name("ns.stream-topic1").build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder().name("ns.stream-topic2").build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(Metadata.builder().name("ns.stream-topic3").build())
                .build();

        List<Topic> allTopics = List.of(topic1, topic2, topic3);
        when(applicationContext.getBean(eq(AccessControlEntryAsyncExecutor.class), any()))
                .thenReturn(aceAsyncExecutor);
        when(topicService.findByWildcardName(eq(namespace), anyString())).thenReturn(allTopics);

        streamService.delete(namespace, stream);

        verify(applicationContext).getBean(eq(AccessControlEntryAsyncExecutor.class), any());
        verify(aceAsyncExecutor).deleteKafkaStreams(namespace, stream);
        verify(topicService).deleteTopics(allTopics);
        verify(streamRepository).delete(stream);
    }
}
