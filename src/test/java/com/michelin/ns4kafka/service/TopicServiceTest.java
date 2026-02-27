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
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TopicRepository;
import com.michelin.ns4kafka.service.executor.TopicAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicServiceTest {
    @InjectMocks
    TopicService topicService;

    @Mock
    AclService aclService;

    @Mock
    TopicRepository topicRepository;

    @Mock
    ApplicationContext applicationContext;

    @Mock
    TopicAsyncExecutor topicAsyncExecutor;

    @Mock
    List<ManagedClusterProperties> managedClusterProperties;

    @Test
    void shouldFindByName() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(Metadata.builder().name("ns1-topic1").build())
                .build();

        when(topicRepository.findAllForCluster("local")).thenReturn(List.of(t1, t2, t3));
        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("ns-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("ns1-topic1")
                                        .build())
                                .build()));
        when(aclService.isResourceCoveredByAcls(any(), anyString())).thenReturn(true);

        // search topic by name
        Optional<Topic> actualTopicPrefixed = topicService.findByName(ns, "ns-topic1");
        assertEquals(actualTopicPrefixed.orElse(Topic.builder().build()), t1);

        Optional<Topic> actualTopicLiteral = topicService.findByName(ns, "ns1-topic1");
        assertEquals(actualTopicLiteral.orElse(Topic.builder().build()), t3);

        Optional<Topic> actualTopicNotFound = topicService.findByName(ns, "ns2-topic1");
        assertThrows(NoSuchElementException.class, actualTopicNotFound::get, "No value present");
    }

    @Test
    void shouldFindAllForNamespaceWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(List.of());

        when(topicRepository.findAllForCluster("local")).thenReturn(List.of());

        assertTrue(topicService.findAllForNamespace(ns).isEmpty());
    }

    @Test
    void shouldFindAllForNamespaceWhenNoAcl() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(Metadata.builder().name("ns1-topic1").build())
                .build();

        Topic t4 = Topic.builder()
                .metadata(Metadata.builder().name("ns2-topic1").build())
                .build();

        when(topicRepository.findAllForCluster("local")).thenReturn(List.of(t1, t2, t3, t4));

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(List.of());

        when(aclService.isResourceCoveredByAcls(any(), anyString())).thenReturn(false);

        assertTrue(topicService.findAllForNamespace(ns).isEmpty());
    }

    @Test
    void shouldFindAllForNamespaceWhenNoAclOnTopic() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(Metadata.builder().name("ns1-topic1").build())
                .build();

        Topic t4 = Topic.builder()
                .metadata(Metadata.builder().name("ns2-topic1").build())
                .build();

        when(topicRepository.findAllForCluster("local")).thenReturn(List.of(t1, t2, t3, t4));

        // All Ns4Kafka access control entries are not for the topics
        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("ns-topic5")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("ns0-")
                                        .build())
                                .build()));

        when(aclService.isResourceCoveredByAcls(any(), anyString())).thenReturn(false);

        assertTrue(topicService.findAllForNamespace(ns).isEmpty());
    }

    @Test
    void shouldImportTopics() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(applicationContext.getBean(
                        TopicAsyncExecutor.class,
                        Qualifiers.byName(ns.getMetadata().getCluster())))
                .thenReturn(topicAsyncExecutor);

        List<Topic> topics = List.of(Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build());

        topicService.importTopics(ns, topics);

        verify(topicAsyncExecutor).importTopics(topics);
    }

    @Test
    void shouldFindAllForNamespace() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic t0 = Topic.builder()
                .metadata(Metadata.builder().name("ns0-topic1").build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(Metadata.builder().name("ns1-topic1").build())
                .build();

        Topic t4 = Topic.builder()
                .metadata(Metadata.builder().name("ns2-topic1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("ns0-topic1")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("ns-")
                                .build())
                        .build());

        when(topicRepository.findAllForCluster("local")).thenReturn(List.of(t0, t1, t2, t3, t4));
        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(acls);
        when(aclService.isResourceCoveredByAcls(acls, "ns1-topic1")).thenReturn(false);
        when(aclService.isResourceCoveredByAcls(acls, "ns2-topic1")).thenReturn(false);
        when(aclService.isResourceCoveredByAcls(acls, "ns0-topic1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns-topic1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns-topic2")).thenReturn(true);

        assertEquals(List.of(t0, t1, t2), topicService.findAllForNamespace(ns));
    }

    @Test
    void shouldListUnsynchronizedTopics() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(applicationContext.getBean(
                        TopicAsyncExecutor.class,
                        Qualifiers.byName(ns.getMetadata().getCluster())))
                .thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of("ns-topic1", "ns-topic2", "ns1-topic1", "ns2-topic1"));

        // list of existing ns4kfk access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic2"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns1-topic1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns2-topic1"))
                .thenReturn(false);

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("ns-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("ns1-topic1")
                                        .build())
                                .build()));

        // no topic exists into ns4kfk
        when(topicRepository.findAllForCluster("local")).thenReturn(List.of());

        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        assertEquals(3, actual.size());

        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic1")));
        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic2")));
        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns1-topic1")));

        assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns2-topic1")));
    }

    @Test
    void shouldListUnsynchronizedWhenAllExistingTopics()
            throws InterruptedException, ExecutionException, TimeoutException {
        // init ns4kfk namespace
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(Metadata.builder().name("ns1-topic1").build())
                .build();

        Topic t4 = Topic.builder()
                .metadata(Metadata.builder().name("ns2-topic1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("ns1-topic1")
                                .build())
                        .build());

        when(applicationContext.getBean(
                        TopicAsyncExecutor.class,
                        Qualifiers.byName(ns.getMetadata().getCluster())))
                .thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of(
                        t1.getMetadata().getName(),
                        t2.getMetadata().getName(),
                        t3.getMetadata().getName(),
                        t4.getMetadata().getName()));

        // list of existing ns4kfk access control entries
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace",
                        AccessControlEntry.ResourceType.TOPIC,
                        t1.getMetadata().getName()))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace",
                        AccessControlEntry.ResourceType.TOPIC,
                        t2.getMetadata().getName()))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace",
                        AccessControlEntry.ResourceType.TOPIC,
                        t3.getMetadata().getName()))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource(
                        "namespace",
                        AccessControlEntry.ResourceType.TOPIC,
                        t4.getMetadata().getName()))
                .thenReturn(false);

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(acls);

        // all topic exists into ns4kfk
        when(topicRepository.findAllForCluster("local")).thenReturn(List.of(t1, t2, t3, t4));

        when(aclService.isResourceCoveredByAcls(acls, "ns-topic1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns-topic2")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns1-topic1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "ns2-topic1")).thenReturn(false);

        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        assertEquals(0, actual.size());
    }

    @Test
    void shouldListUnsynchronizedWhenNotAllTopicsAlreadyExist()
            throws InterruptedException, ExecutionException, TimeoutException {
        // init ns4kfk namespace
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("ns-")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("ns1-topic1")
                                .build())
                        .build());

        when(applicationContext.getBean(
                        TopicAsyncExecutor.class,
                        Qualifiers.byName(ns.getMetadata().getCluster())))
                .thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of("ns-topic1", "ns-topic2", "ns1-topic1", "ns2-topic1"));

        // list of existing ns4kfk access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic2"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns1-topic1"))
                .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns2-topic1"))
                .thenReturn(false);

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(acls);

        // partial number of topics exists into ns4kfk
        when(topicRepository.findAllForCluster("local")).thenReturn(List.of(t1));
        when(aclService.isResourceCoveredByAcls(acls, "ns-topic1")).thenReturn(true);

        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        assertEquals(2, actual.size());

        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic2")));
        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns1-topic1")));

        assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns-topic1")));
        assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns2-topic1")));
    }

    @Test
    void shouldNotFindAnyCollidingTopic() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("project1.topic").build())
                .build();

        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("project2.topic", "project1.other"));

        assertTrue(topicService.findCollidingTopics(ns, topic).isEmpty());
    }

    @Test
    void shouldFindCollidingTopicWhenIdenticalName() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("project1.topic").build())
                .build();

        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of("project1.topic", "project2.topic", "project1.other"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        assertTrue(actual.isEmpty(), "Topic with exactly the same name should not interfere with collision check");
    }

    @Test
    void shouldFindCollidingTopicsWhenCollidingName()
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("project1.topic").build())
                .build();

        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("project1_topic"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("project1_topic"), actual);
    }

    @Test
    void shouldHandleInterruptedExceptionWhenFindingCollidingTopics()
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("project1.topic").build())
                .build();

        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames()).thenThrow(new InterruptedException());

        assertThrows(InterruptedException.class, () -> topicService.findCollidingTopics(ns, topic));

        assertTrue(Thread.interrupted());
    }

    @Test
    void shouldHandleOtherExceptionWhenFindingCollidingTopics()
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("project1.topic").build())
                .build();

        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames()).thenThrow(new RuntimeException("Unknown Error"));

        assertThrows(RuntimeException.class, () -> topicService.findCollidingTopics(ns, topic));
    }

    @Test
    void shouldFindTopicsWithNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder().name("prefix.topic1").build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder().name("prefix.topic2").build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(Metadata.builder().name("prefix.topic3").build())
                .build();

        Topic topic4 = Topic.builder()
                .metadata(Metadata.builder().name("prefix2.topic").build())
                .build();

        List<AccessControlEntry> acls = List.of(AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("prefix.")
                        .build())
                .build());

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(acls);
        when(topicRepository.findAllForCluster("local")).thenReturn(List.of(topic1, topic2, topic3, topic4));
        when(aclService.isResourceCoveredByAcls(acls, "prefix.topic1")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "prefix.topic2")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "prefix.topic3")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "prefix2.topic")).thenReturn(false);

        assertEquals(List.of(topic1, topic2, topic3), topicService.findByWildcardName(ns, ""));
        assertEquals(List.of(topic2), topicService.findByWildcardName(ns, "prefix.topic2"));
        assertTrue(topicService.findByWildcardName(ns, "topic2.suffix").isEmpty()); // doesn't exist
        assertTrue(topicService.findByWildcardName(ns, "prefix2.topic").isEmpty()); // no acl
    }

    @Test
    void shouldFindTopicsWithWildcardNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder().name("prefix1.topic1").build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder().name("prefix1.topic2").build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(Metadata.builder().name("prefix1.topic3").build())
                .build();

        Topic topic4 = Topic.builder()
                .metadata(Metadata.builder().name("prefix2.topic1").build())
                .build();

        Topic topic5 = Topic.builder()
                .metadata(Metadata.builder().name("prefix2.topic2").build())
                .build();

        Topic topic6 = Topic.builder()
                .metadata(Metadata.builder().name("topic1").build())
                .build();

        List<Topic> allTopics = List.of(topic1, topic2, topic3, topic4, topic5, topic6);

        when(aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("prefix1.")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("prefix2.")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("topic1")
                                        .build())
                                .build()));
        when(topicRepository.findAllForCluster("local")).thenReturn(allTopics);
        when(aclService.isResourceCoveredByAcls(any(), any())).thenReturn(true);

        // Find one or multiple topics with wildcard
        assertEquals(List.of(topic1, topic2, topic3), topicService.findByWildcardName(ns, "prefix1.*"));
        assertEquals(List.of(topic1, topic2, topic3), topicService.findByWildcardName(ns, "prefix1.topic?"));
        assertEquals(List.of(topic1, topic4, topic6), topicService.findByWildcardName(ns, "*topic1"));
        assertEquals(List.of(topic1, topic4), topicService.findByWildcardName(ns, "*.topic1"));
        assertEquals(List.of(topic2, topic5), topicService.findByWildcardName(ns, "prefix?.topic2"));
        assertEquals(List.of(topic1, topic2, topic3, topic4, topic5), topicService.findByWildcardName(ns, "*.topic?"));
        assertEquals(List.of(topic1, topic4, topic6), topicService.findByWildcardName(ns, "*topic1*"));
        assertEquals(List.of(topic1, topic2, topic3, topic4, topic5), topicService.findByWildcardName(ns, "*.*"));
        assertEquals(allTopics, topicService.findByWildcardName(ns, "*"));
        assertEquals(allTopics, topicService.findByWildcardName(ns, "********"));
        assertEquals(List.of(topic6), topicService.findByWildcardName(ns, "??????")); // 6-characters topic

        // Find no topics with wildcard
        assertTrue(topicService.findByWildcardName(ns, "prefix3.*").isEmpty()); // no ACL
        assertTrue(topicService.findByWildcardName(ns, "prefix4.*").isEmpty()); // doesn't exist
        assertTrue(topicService.findByWildcardName(ns, "*.???").isEmpty());
        assertTrue(topicService.findByWildcardName(ns, ".*").isEmpty()); // .* is regex
        assertTrue(topicService.findByWildcardName(ns, "......").isEmpty()); // . is regex
    }

    @Test
    void shouldValidateDeleteRecords() {
        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("project1.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Collections.singletonMap("cleanup.policy", "compact"))
                        .build())
                .build();

        List<String> actual = topicService.validateDeleteRecordsTopic(topic);

        assertEquals(1, actual.size());
        assertLinesMatch(
                List.of("Invalid \"delete records\" operation: cannot delete records on a compacted topic. "
                        + "Please delete and recreate the topic."),
                actual);
    }

    @Test
    void shouldValidateDeleteRecordsWhenEmptyCleanUpPolicy() {
        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("project1.topic").build())
                .spec(Topic.TopicSpec.builder().configs(Collections.emptyMap()).build())
                .build();

        List<String> actual = topicService.validateDeleteRecordsTopic(topic);

        assertEquals(0, actual.size());
    }

    @Test
    void shouldNotValidateUpdateTopicPartitions() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic existing = Topic.builder()
                .metadata(Metadata.builder()
                        .name("test.topic")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy", "compact", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(6)
                        .configs(Map.of(
                                "cleanup.policy", "compact", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of());

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Invalid value \"6\" for field \"partitions\": value is immutable."), actual);
    }

    @Test
    void shouldNotValidateUpdateTopicReplicationFactor() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic existing = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy", "compact", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(6)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy", "compact", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of());

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Invalid value \"6\" for field \"replication.factor\": value is immutable."), actual);
    }

    @Test
    void shouldValidateUpdateTopicWhenEmptyCleanUpPolicy() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic existing = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("min.insync.replicas", "2", "retention.ms", "70000"))
                        .build())
                .build();

        when(managedClusterProperties.stream())
                .thenReturn(Stream.of(
                        new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(0, actual.size());
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "compact;delete;CONFLUENT_CLOUD",
                "delete;compact;CONFLUENT_CLOUD",
                "compact;delete,compact;CONFLUENT_CLOUD",
                "delete,compact;compact;CONFLUENT_CLOUD",
                "delete,compact;delete;CONFLUENT_CLOUD",
                "compact;delete;SELF_MANAGED",
                "delete;compact;SELF_MANAGED",
                "compact;delete,compact;SELF_MANAGED",
                "delete;delete,compact;SELF_MANAGED",
                "delete,compact;compact;SELF_MANAGED",
                "delete,compact;delete;SELF_MANAGED"
            },
            delimiterString = ";")
    void shouldValidateUpdateTopicCleanUpPolicy(
            String oldCleanUpPolicy, String newCleanUpPolicy, ManagedClusterProperties.KafkaProvider provider) {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic existing = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy",
                                oldCleanUpPolicy,
                                "min.insync.replicas",
                                "2",
                                "retention.ms",
                                "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy",
                                newCleanUpPolicy,
                                "min.insync.replicas",
                                "2",
                                "retention.ms",
                                "60000"))
                        .build())
                .build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of(new ManagedClusterProperties("local", provider)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(0, actual.size());
    }

    @ParameterizedTest
    @CsvSource(
            value = {"delete;delete,compact;CONFLUENT_CLOUD", "delete;compact,delete;CONFLUENT_CLOUD"},
            delimiterString = ";")
    void shouldNotValidateUpdateTopicCleanUpPolicy(String oldCleanUpPolicy, String newCleanUpPolicy) {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic existing = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy",
                                oldCleanUpPolicy,
                                "min.insync.replicas",
                                "2",
                                "retention.ms",
                                "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy",
                                newCleanUpPolicy,
                                "min.insync.replicas",
                                "2",
                                "retention.ms",
                                "60000"))
                        .build())
                .build();

        when(managedClusterProperties.stream())
                .thenReturn(Stream.of(
                        new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(
                List.of("Invalid value \"" + newCleanUpPolicy
                        + "\" for field \"cleanup.policy\": altering topic configuration "
                        + "from \"delete\" to \"compact\" and \"delete\" is not currently supported in Confluent Cloud. "
                        + "Please create a new topic instead."),
                actual);
    }

    @Test
    void shouldFindAllTopics() {
        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(Metadata.builder().name("ns1-topic1").build())
                .build();

        Topic t4 = Topic.builder()
                .metadata(Metadata.builder().name("ns2-topic1").build())
                .build();

        when(topicRepository.findAll()).thenReturn(List.of(t1, t2, t3, t4));

        Collection<Topic> topics = topicService.findAll();
        assertEquals(4, topics.size());
    }

    @Test
    void shouldDeleteTopic() throws ExecutionException, InterruptedException, TimeoutException {
        Topic topic = Topic.builder()
                .metadata(
                        Metadata.builder().name("ns-topic1").cluster("cluster").build())
                .build();

        when(applicationContext.getBean(eq(TopicAsyncExecutor.class), any())).thenReturn(topicAsyncExecutor);

        topicService.delete(topic);

        verify(topicRepository).delete(topic);
        verify(topicAsyncExecutor).deleteTopics(List.of(topic));
    }

    @Test
    void shouldDeleteMultipleTopics() throws ExecutionException, InterruptedException, TimeoutException {
        Topic topic1 = Topic.builder()
                .metadata(
                        Metadata.builder().name("ns-topic1").cluster("cluster").build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(
                        Metadata.builder().name("ns-topic2").cluster("cluster").build())
                .build();

        List<Topic> topics = List.of(topic1, topic2);

        when(applicationContext.getBean(eq(TopicAsyncExecutor.class), any())).thenReturn(topicAsyncExecutor);

        topicService.deleteTopics(topics);

        verify(topicAsyncExecutor).deleteTopics(topics);
        verify(topicRepository).delete(topic1);
        verify(topicRepository).delete(topic2);
    }

    @Test
    void shouldListUnsynchronizedTopicNames() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        when(applicationContext.getBean(eq(TopicAsyncExecutor.class), any())).thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2", "ns2-topic1"));
        when(aclService.isNamespaceOwnerOfResource(any(), any(), any()))
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        when(topicAsyncExecutor.collectBrokerTopicsFromNames(List.of("ns-topic1", "ns-topic2")))
                .thenReturn(Map.of("ns-topic1", t1, "ns-topic2", t2));

        List<Topic> actual = topicService.listUnsynchronizedTopicsByWildcardName(ns, "*");

        assertEquals(2, actual.size());
        assertTrue(actual.contains(t1));
        assertTrue(actual.contains(t2));
    }

    @Test
    void shouldListUnsynchronizedTopicNamesWithWildcardParameter()
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic12").build())
                .build();

        when(applicationContext.getBean(eq(TopicAsyncExecutor.class), any())).thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2", "ns-topic12"));
        when(aclService.isNamespaceOwnerOfResource(any(), any(), any()))
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true);
        when(topicAsyncExecutor.collectBrokerTopicsFromNames(List.of("ns-topic1", "ns-topic2", "ns-topic12")))
                .thenReturn(Map.of("ns-topic1", t1, "ns-topic2", t2, "ns-topic12", t3));

        List<Topic> actual = topicService.listUnsynchronizedTopicsByWildcardName(ns, "ns-topic1*");

        assertEquals(2, actual.size());
        assertTrue(actual.contains(t1));
        assertTrue(actual.contains(t3));
    }

    @Test
    void shouldListUnsynchronizedTopicNamesWithNameParameter()
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .build();

        when(applicationContext.getBean(eq(TopicAsyncExecutor.class), any())).thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2", "ns2-topic1"));
        when(aclService.isNamespaceOwnerOfResource(any(), any(), any()))
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        when(topicAsyncExecutor.collectBrokerTopicsFromNames(List.of("ns-topic1", "ns-topic2")))
                .thenReturn(Map.of("ns-topic1", t1, "ns-topic2", t2));

        List<Topic> actual = topicService.listUnsynchronizedTopicsByWildcardName(ns, "ns-topic1");

        assertEquals(1, actual.size());
        assertTrue(actual.contains(t1));
        assertFalse(actual.contains(t2));
    }

    @Test
    void shouldTagsBeValid() {
        ManagedClusterProperties managedClusterProps =
                new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        Properties properties = new Properties();
        managedClusterProps.setConfig(properties);

        when(managedClusterProperties.stream()).thenReturn(Stream.of(managedClusterProps));

        List<String> validationErrors = topicService.validateTags(
                Namespace.builder()
                        .metadata(Metadata.builder()
                                .name("namespace")
                                .cluster("local")
                                .build())
                        .build(),
                Topic.builder()
                        .metadata(Metadata.builder().name("ns-topic1").build())
                        .spec(Topic.TopicSpec.builder()
                                .tags(List.of("TAG_TEST"))
                                .build())
                        .build());

        assertEquals(0, validationErrors.size());
    }

    @Test
    void shouldTagsBeInvalidWhenNotConfluentCloud() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("TAG_TEST")).build())
                .build();

        when(managedClusterProperties.stream())
                .thenReturn(Stream.of(
                        new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.SELF_MANAGED)));

        List<String> validationErrors = topicService.validateTags(ns, topic);
        assertEquals(1, validationErrors.size());
        assertEquals(
                "Invalid value \"TAG_TEST\" for field \"tags\": tags are not currently supported.",
                validationErrors.getFirst());
    }

    @Test
    void shouldReturnTrueWhenTagFormatIsValid() {
        Topic topicWithTag = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("test")).build())
                .build();

        assertTrue(topicService.isTagsFormatValid(topicWithTag));

        Topic topicWithEasiestTag = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("A")).build())
                .build();

        assertTrue(topicService.isTagsFormatValid(topicWithEasiestTag));

        Topic topicWithUnderscoreAndNumberTag = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic3").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("TEST1_TAG")).build())
                .build();

        assertTrue(topicService.isTagsFormatValid(topicWithUnderscoreAndNumberTag));

        Topic topicWithUnderscoreAndUpperLowerCase = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic4").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("t1_T_a_g2")).build())
                .build();

        assertTrue(topicService.isTagsFormatValid(topicWithUnderscoreAndUpperLowerCase));

        Topic topicWithMultipleCorrectTags = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic5").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of("TEST1", "test2", "tEST_3", "T_a_g"))
                        .build())
                .build();

        assertTrue(topicService.isTagsFormatValid(topicWithMultipleCorrectTags));
    }

    @Test
    void shouldReturnFalseWhenTagFormatIsInvalid() {
        Topic topicWithBeginningDigitTag = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("0test")).build())
                .build();

        assertFalse(topicService.isTagsFormatValid(topicWithBeginningDigitTag));

        Topic topicWithBeginningUnderscoreTag = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic2").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("_TEST")).build())
                .build();

        assertFalse(topicService.isTagsFormatValid(topicWithBeginningUnderscoreTag));

        Topic topicWithForbiddenCharacterTag = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic3").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("test-tag")).build())
                .build();

        assertFalse(topicService.isTagsFormatValid(topicWithForbiddenCharacterTag));

        Topic topicWithManyForbiddenCharactersTag = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic4").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of("&~#()[]{}-+=*%:.,;!?^°çé"))
                        .build())
                .build();

        assertFalse(topicService.isTagsFormatValid(topicWithManyForbiddenCharactersTag));

        Topic topicWithMultipleIncorrectTags = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic5").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of("test-tag", "TEST.tag", "0TEST"))
                        .build())
                .build();

        assertFalse(topicService.isTagsFormatValid(topicWithMultipleIncorrectTags));

        Topic topicWithOneIncorrectAndMultipleCorrectTags = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic5").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of("testTag", "0TEST-tag", "TEST"))
                        .build())
                .build();

        assertFalse(topicService.isTagsFormatValid(topicWithOneIncorrectAndMultipleCorrectTags));
    }

    @Test
    void shouldTagsBeInvalidWhenFormatIsWrong() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("ns-topic1").build())
                .spec(Topic.TopicSpec.builder().tags(List.of("0TAG-TEST")).build())
                .build();

        when(managedClusterProperties.stream())
                .thenReturn(Stream.of(
                        new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD)));

        List<String> validationErrors = topicService.validateTags(ns, topic);
        assertEquals(1, validationErrors.size());
        assertEquals(
                "Invalid value \"0TAG-TEST\" for field \"tags\": "
                        + "tags should start with letter and be followed by alphanumeric or _ characters.",
                validationErrors.getFirst());
    }

    @Test
    void deleteTopics_shouldNotCallAnything_whenListIsNull() throws Exception {
        topicService.deleteTopics(null);

        verify(topicAsyncExecutor, never()).deleteTopics(any());
        verify(topicRepository, never()).delete(any());
    }

    @Test
    void deleteTopics_shouldNotCallAnything_whenListIsEmpty() throws Exception {
        topicService.deleteTopics(List.of());

        verify(topicAsyncExecutor, never()).deleteTopics(any());
        verify(topicRepository, never()).delete(any());
    }
}
