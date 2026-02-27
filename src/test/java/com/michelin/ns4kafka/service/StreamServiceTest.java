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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.repository.StreamRepository;
import com.michelin.ns4kafka.service.executor.AccessControlEntryAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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

        KafkaStream stream = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("stream")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        when(streamRepository.findByName(any(), any())).thenReturn(Optional.of(stream));

        var actual = streamService.findByName(ns, "stream");

        assertTrue(actual.isPresent());
        assertEquals(stream, actual.get());
    }

    @Test
    void shouldReturnEmptyWhenFindByNameByAnotherNamespace() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("malicious-namespace")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("stream")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .build();

        when(streamRepository.findByName(any(), any())).thenReturn(Optional.of(stream));

        var actual = streamService.findByName(ns, "stream");

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
    void shouldNamespaceHaveKafkaStreams() {
        Namespace ns1 = Namespace.builder()
                .metadata(Metadata.builder().name("test1").cluster("local").build())
                .build();

        Namespace ns2 = Namespace.builder()
                .metadata(Metadata.builder().name("test2").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream1")
                        .namespace("test1")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream3 = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("test_stream3")
                        .namespace("test3")
                        .cluster("local")
                        .build())
                .build();

        when(streamRepository.findAllForCluster(any())).thenReturn(List.of(stream1, stream3));

        assertTrue(streamService.hasKafkaStream(ns1));
        assertFalse(streamService.hasKafkaStream(ns2));
    }

    @Test
    void shouldDeleteKafkaStreamAndRelatedTopics() throws Exception {
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder().name("ns").cluster("local").build())
                .build();

        KafkaStream stream = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1")
                        .namespace("ns")
                        .cluster("local")
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-topic1-repartition")
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-topic1-changelog")
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-topic1-norepartition")
                        .build())
                .build();

        Topic topic4 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-topic1-nochangelog")
                        .build())
                .build();

        Topic topic5 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix2.stream_app_id2-topic1-norepartition")
                        .build())
                .build();

        Topic topic6 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix2.stream_app_id2-topic2-nochangelog")
                        .build())
                .build();

        Topic topic7 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-sub-appid-overlap-topic1-repartition")
                        .build())
                .build();

        KafkaStream kafkaStream = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .namespace("ns")
                        .name("prefix1.stream_app_id1-sub-appid-overlap")
                        .build())
                .build();

        when(applicationContext.getBean(eq(AccessControlEntryAsyncExecutor.class), any()))
                .thenReturn(aceAsyncExecutor);
        List<KafkaStream> kafkaStreams = List.of(kafkaStream);
        when(streamRepository.findAllForCluster(any())).thenReturn(kafkaStreams);

        List<Topic> allTopics = List.of(topic1, topic2, topic3, topic4, topic5, topic6, topic7);
        when(topicService.findByWildcardName(eq(namespace), anyString())).thenReturn(allTopics);

        streamService.delete(namespace, stream);
        verify(aceAsyncExecutor).deleteKafkaStreams(namespace, stream);
        verify(topicService)
                .deleteTopics(argThat(topics -> topics.stream().anyMatch(topic -> topic.getMetadata()
                                .getName()
                                .equals("prefix1.stream_app_id1-topic1-repartition"))
                        && topics.stream()
                                .anyMatch(topic ->
                                        topic.getMetadata().getName().equals("prefix1.stream_app_id1-topic1-changelog"))
                        && topics.stream().noneMatch(topic -> topic.getMetadata()
                                .getName()
                                .equals("prefix1.stream_app_id1-topic1-norepartition"))
                        && topics.stream().noneMatch(topic -> topic.getMetadata()
                                .getName()
                                .equals("prefix1.stream_app_id1-topic1-nochangelog"))
                        && topics.stream()
                                .noneMatch(
                                        topic -> topic.getMetadata().getName().startsWith("prefix2.stream_app_id2"))
                        && topics.stream().noneMatch(topic -> topic.getMetadata()
                                .getName()
                                .startsWith("prefix1.stream_app_id1-sub-appid-topic1-repartition"))));

        verify(streamRepository).delete(stream);
    }

    @Test
    void shouldDeleteKafkaStreamAndRelatedTopicsWhenOverlapKafkaStreamsIsEmpty() throws Exception {
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder().name("ns").cluster("local").build())
                .build();

        KafkaStream stream = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("prefix.stream_app_id")
                        .namespace("ns")
                        .cluster("local")
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-topic1-repartition")
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-topic1-changelog")
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-topic1-norepartition")
                        .build())
                .build();

        Topic topic4 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix1.stream_app_id1-topic1-nochangelog")
                        .build())
                .build();

        Topic topic5 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix2.stream_app_id2-topic1-norepartition")
                        .build())
                .build();

        Topic topic6 = Topic.builder()
                .metadata(Metadata.builder()
                        .name("prefix2.stream_app_id2-topic2-nochangelog")
                        .build())
                .build();

        when(applicationContext.getBean(eq(AccessControlEntryAsyncExecutor.class), any()))
                .thenReturn(aceAsyncExecutor);
        when(streamRepository.findAllForCluster(any())).thenReturn(Collections.emptyList());

        List<Topic> allTopics = List.of(topic1, topic2, topic3, topic4, topic5, topic6);
        when(topicService.findByWildcardName(eq(namespace), anyString())).thenReturn(allTopics);

        streamService.delete(namespace, stream);
        verify(aceAsyncExecutor).deleteKafkaStreams(namespace, stream);
        verify(topicService)
                .deleteTopics(argThat(topics -> topics.stream().anyMatch(topic -> topic.getMetadata()
                                .getName()
                                .equals("prefix1.stream_app_id1-topic1-repartition"))
                        && topics.stream()
                                .anyMatch(topic ->
                                        topic.getMetadata().getName().equals("prefix1.stream_app_id1-topic1-changelog"))
                        && topics.stream().noneMatch(topic -> topic.getMetadata()
                                .getName()
                                .equals("prefix1.stream_app_id1-topic1-norepartition"))
                        && topics.stream().noneMatch(topic -> topic.getMetadata()
                                .getName()
                                .equals("prefix1.stream_app_id1-topic1-nochangelog"))
                        && topics.stream()
                                .noneMatch(
                                        topic -> topic.getMetadata().getName().startsWith("prefix2.stream_app_id2"))
                        && topics.stream().noneMatch(topic -> topic.getMetadata()
                                .getName()
                                .startsWith("prefix1.stream_app_id1-sub-appid-topic1-repartition"))));

        verify(streamRepository).delete(stream);
    }

    @Test
    void shouldNotCallDeleteTopicsWhenStreamTopicListIsEmpty() throws Exception {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("ns").cluster("local").build())
                .build();

        KafkaStream stream = KafkaStream.builder()
                .metadata(Metadata.builder()
                        .name("prefix.stream_app_id")
                        .namespace("ns")
                        .cluster("local")
                        .build())
                .build();

        when(applicationContext.getBean(eq(AccessControlEntryAsyncExecutor.class), any()))
                .thenReturn(aceAsyncExecutor);
        when(topicService.findByWildcardName(eq(ns), anyString())).thenReturn(List.of()); // No topics

        streamService.delete(ns, stream);

        verify(topicService, never()).deleteTopics(any());
        verify(streamRepository).delete(stream);
    }
}
