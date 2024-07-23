package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
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

    /**
     * Validate find topic by name.
     */
    @Test
    void findByName() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
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

        when(topicRepository.findAllForCluster("local"))
            .thenReturn(List.of(t1, t2, t3, t4));

        when(aclService.findAllGrantedToNamespace(ns))
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
                    .build()
            ));

        // search topic by name
        Optional<Topic> actualTopicPrefixed = topicService.findByName(ns, "ns-topic1");
        assertEquals(actualTopicPrefixed.get(), t1);

        Optional<Topic> actualTopicLiteral = topicService.findByName(ns, "ns1-topic1");
        assertEquals(actualTopicLiteral.get(), t3);

        Optional<Topic> actualTopicNotFound = topicService.findByName(ns, "ns2-topic1");
        assertThrows(NoSuchElementException.class, actualTopicNotFound::get, "No value present");
    }

    @Test
    void findAllForNamespaceNoTopics() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        // no ns4kfk access control entries
        when(aclService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of());

        // no ns4kfk topics 
        when(topicRepository.findAllForCluster("local"))
            .thenReturn(List.of());

        // get list of topics
        List<Topic> list = topicService.findAllForNamespace(ns);

        // list of topics is empty
        assertTrue(list.isEmpty());
    }

    @Test
    void findAllForNamespaceNoAcls() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        // init ns4kfk topics
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
        when(topicRepository.findAllForCluster("local"))
            .thenReturn(List.of(t1, t2, t3, t4));

        // no ns4kfk access control entries
        when(aclService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of());

        // list of topics is empty 
        List<Topic> actual = topicService.findAllForNamespace(ns);
        assertTrue(actual.isEmpty());
    }

    @Test
    void findAllForNamespace() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
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
        when(topicRepository.findAllForCluster("local"))
            .thenReturn(List.of(t0, t1, t2, t3, t4));

        when(aclService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of(
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
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1-topic1")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns2-topic1")
                        .build())
                    .build()
            ));


        // search for topics into namespace
        List<Topic> actual = topicService.findAllForNamespace(ns);

        assertEquals(3, actual.size());
        // contains
        assertTrue(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns0-topic1")));
        assertTrue(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns-topic1")));
        assertTrue(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns-topic2")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns1-topic1")));
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns2-topic1")));
    }

    @Test
    void listUnsynchronizedNoExistingTopics() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        // init topicAsyncExecutor
        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2",
            "ns1-topic1", "ns2-topic1"));

        // list of existing ns4kfk access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            "ns-topic1"))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            "ns-topic2"))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            "ns1-topic1"))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            "ns2-topic1"))
            .thenReturn(false);

        when(aclService.findAllGrantedToNamespace(ns))
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
                    .build()
            ));

        // no topic exists into ns4kfk
        when(topicRepository.findAllForCluster("local"))
            .thenReturn(List.of());
        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        assertEquals(3, actual.size());
        // contains
        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic1")));
        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic2")));
        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns1-topic1")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns2-topic1")));

    }

    @Test
    void listUnsynchronizedAllExistingTopics() throws InterruptedException, ExecutionException, TimeoutException {
        // init ns4kfk namespace
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
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

        // init topicAsyncExecutor
        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(
            List.of(t1.getMetadata().getName(), t2.getMetadata().getName(),
                t3.getMetadata().getName(), t4.getMetadata().getName()));

        // list of existing ns4kfk access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            t1.getMetadata().getName()))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            t2.getMetadata().getName()))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            t3.getMetadata().getName()))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            t4.getMetadata().getName()))
            .thenReturn(false);

        when(aclService.findAllGrantedToNamespace(ns))
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
                    .build()
            ));

        // all topic exists into ns4kfk
        when(topicRepository.findAllForCluster("local"))
            .thenReturn(List.of(t1, t2, t3, t4));

        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        assertEquals(0, actual.size());

    }

    @Test
    void listUnsynchronizedPartialExistingTopics() throws InterruptedException, ExecutionException, TimeoutException {
        // init ns4kfk namespace
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Topic t1 = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic1").build())
            .build();

        // init topicAsyncExecutor
        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class,
            Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2",
            "ns1-topic1", "ns2-topic1"));

        // list of existing ns4kfk access control entries
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            "ns-topic1"))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            "ns-topic2"))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            "ns1-topic1"))
            .thenReturn(true);
        when(aclService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC,
            "ns2-topic1"))
            .thenReturn(false);

        when(aclService.findAllGrantedToNamespace(ns))
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
                    .build()
            ));

        // partial number of topics exists into ns4kfk
        when(topicRepository.findAllForCluster("local"))
            .thenReturn(List.of(t1));

        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        assertEquals(2, actual.size());
        // contains
        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic2")));
        assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns1-topic1")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns-topic1")));
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns2-topic1")));

    }

    @Test
    void findCollidingTopicsNoCollision() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();
        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("project1.topic").build())
            .build();

        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
            .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
            .thenReturn(List.of("project2.topic", "project1.other"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        assertTrue(actual.isEmpty());
    }

    @Test
    void findCollidingTopicsIdenticalName() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();
        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("project1.topic").build())
            .build();

        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
            .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
            .thenReturn(List.of("project1.topic", "project2.topic", "project1.other"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        assertTrue(actual.isEmpty(), "Topic with exactly the same name should not interfere with collision check");
    }

    @Test
    void findCollidingTopicsCollidingName() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();
        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("project1.topic").build())
            .build();

        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
            .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
            .thenReturn(List.of("project1_topic"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("project1_topic"), actual);
    }

    @Test
    void findCollidingTopicsInterruptedException() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();
        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("project1.topic").build())
            .build();

        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
            .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
            .thenThrow(new InterruptedException());

        assertThrows(InterruptedException.class,
            () -> topicService.findCollidingTopics(ns, topic));

        assertTrue(Thread.interrupted());
    }

    @Test
    void findCollidingTopicsOtherException() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("project1.topic").build())
            .build();

        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
            .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
            .thenThrow(new RuntimeException("Unknown Error"));

        assertThrows(RuntimeException.class,
            () -> topicService.findCollidingTopics(ns, topic));
    }

    @Test
    void validateDeleteRecordsTopic() {
        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("project1.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .configs(Collections.singletonMap("cleanup.policy", "compact"))
                .build())
            .build();

        List<String> actual = topicService.validateDeleteRecordsTopic(topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Invalid \"delete records\" operation: cannot delete records on a compacted topic. "
                + "Please delete and recreate the topic."),
            actual);
    }

    @Test
    void validateTopicUpdatePartitions() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
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
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(6)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of());

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Invalid value \"6\" for field \"partitions\": value is immutable."), actual);
    }

    @Test
    void validateTopicUpdateReplicationFactor() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        Topic existing = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(6)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of());

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Invalid value \"6\" for field \"replication.factor\": value is immutable."),
            actual);
    }

    @Test
    void validateTopicUpdateCleanupPolicyDeleteToCompactOnCloud() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        Topic existing = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(managedClusterProperties.stream()).thenReturn(
            Stream.of(new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of(
                "Invalid value \"compact\" for field \"cleanup.policy\": altering topic cleanup policy "
                    + "from delete to compact is not currently supported in Confluent Cloud. "
                    + "Please create a new topic with compact policy instead."),
            actual);
    }

    @Test
    void validateTopicUpdateCleanupPolicyCompactToDeleteOnCloud() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        Topic existing = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(managedClusterProperties.stream()).thenReturn(
            Stream.of(new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(0, actual.size());
    }

    @Test
    void validateTopicUpdateCleanupPolicyDeleteToCompactOnSelfManaged() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        Topic existing = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(managedClusterProperties.stream()).thenReturn(
            Stream.of(new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.SELF_MANAGED)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(0, actual.size());
    }

    @Test
    void findAll() {
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

        List<Topic> topics = topicService.findAll();
        assertEquals(4, topics.size());
    }

    @Test
    void shouldDelete() throws ExecutionException, InterruptedException, TimeoutException {
        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns-topic1")
                .cluster("cluster")
                .build())
            .build();

        when(applicationContext.getBean(eq(TopicAsyncExecutor.class), any())).thenReturn(topicAsyncExecutor);

        topicService.delete(topic);

        verify(topicRepository).delete(topic);
        verify(topicAsyncExecutor).deleteTopic(topic);
    }

    @Test
    void shouldListUnsynchronizedTopicNames() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        Topic t1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns-topic1")
                .build())
            .build();

        Topic t2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns-topic2")
                .build())
            .build();

        when(applicationContext.getBean(eq(TopicAsyncExecutor.class), any())).thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2", "ns2-topic1"));
        when(aclService.isNamespaceOwnerOfResource(any(), any(), any()))
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(topicAsyncExecutor.collectBrokerTopicsFromNames(List.of("ns-topic1", "ns-topic2"))).thenReturn(
            Map.of("ns-topic1", t1, "ns-topic2", t2));

        List<Topic> actual = topicService.listUnsynchronizedTopics(ns);

        assertEquals(2, actual.size());
        assertTrue(actual.contains(t1));
        assertTrue(actual.contains(t2));
    }

    @Test
    void shouldTagsBeValid() {
        ManagedClusterProperties managedClusterProps =
            new ManagedClusterProperties("local",
                ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        Properties properties = new Properties();
        managedClusterProps.setConfig(properties);

        when(managedClusterProperties.stream()).thenReturn(Stream.of(managedClusterProps));

        List<String> validationErrors = topicService.validateTags(
            Namespace.builder().metadata(
                Metadata.builder().name("namespace").cluster("local").build()).build(),
            Topic.builder().metadata(
                Metadata.builder().name("ns-topic1").build()).spec(Topic.TopicSpec.builder()
                .tags(List.of("TAG_TEST")).build()).build());
        assertEquals(0, validationErrors.size());
    }

    @Test
    void shouldTagsBeInvalidWhenNotConfluentCloud() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic1").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("TAG_TEST")).build())
            .build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of(
            new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.SELF_MANAGED)));

        List<String> validationErrors = topicService.validateTags(ns, topic);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value \"TAG_TEST\" for field \"tags\": tags are not currently supported.",
            validationErrors.getFirst());
    }

    @Test
    void shouldReturnTrueWhenTagFormatIsValid() {
        Topic topicWithTag = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic1").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("test")).build())
            .build();

        assertEquals(true, topicService.isTagsFormatValid(topicWithTag));

        Topic topicWithSimpliestTag = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic2").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("A")).build())
            .build();

        assertEquals(true, topicService.isTagsFormatValid(topicWithSimpliestTag));

        Topic topicWithUnderscoreAndNumberTag = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic3").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("TEST1_TAG")).build())
            .build();

        assertEquals(true, topicService.isTagsFormatValid(topicWithUnderscoreAndNumberTag));

        Topic topicWithUnderscoreAndUpperLowerCase = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic4").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("t1_T_a_g2")).build())
            .build();

        assertEquals(true, topicService.isTagsFormatValid(topicWithUnderscoreAndUpperLowerCase));

        Topic topicWithMultipleCorrectTags = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic5").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("TEST1", "test2", "tEST_3", "T_a_g")).build())
            .build();

        assertEquals(true, topicService.isTagsFormatValid(topicWithMultipleCorrectTags));
    }

    @Test
    void shouldReturnFalseWhenTagFormatIsInvalid() {
        Topic topicWithBeginningDigitTag = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic1").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("0test")).build())
            .build();

        assertEquals(false, topicService.isTagsFormatValid(topicWithBeginningDigitTag));

        Topic topicWithBeginningUnderscoreTag = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic2").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("_TEST")).build())
            .build();

        assertEquals(false, topicService.isTagsFormatValid(topicWithBeginningUnderscoreTag));

        Topic topicWithForbiddenCharacterTag = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic3").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("test-tag")).build())
            .build();

        assertEquals(false, topicService.isTagsFormatValid(topicWithForbiddenCharacterTag));

        Topic topicWithManyForbiddenCharactersTag = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic4").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("&~#()[]{}-+=*%:.,;!?^°çé")).build())
            .build();

        assertEquals(false, topicService.isTagsFormatValid(topicWithManyForbiddenCharactersTag));

        Topic topicWithMultipleIncorrectTags = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic5").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("test-tag", "TEST.tag", "0TEST")).build())
            .build();

        assertEquals(false, topicService.isTagsFormatValid(topicWithMultipleIncorrectTags));

        Topic topicWithOneIncorrectAndMultipleCorrectTags = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic5").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("testTag", "0TEST-tag", "TEST")).build())
            .build();

        assertEquals(false, topicService.isTagsFormatValid(topicWithOneIncorrectAndMultipleCorrectTags));
    }

    @Test
    void shouldTagsBeInvalidWhenFormatIsWrong() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("ns-topic1").build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("0TAG-TEST")).build())
            .build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of(
            new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD)));

        List<String> validationErrors = topicService.validateTags(ns, topic);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value \"0TAG-TEST\" for field \"tags\": "
                + "tags should start with letter and be followed by alphanumeric or _ characters.",
            validationErrors.getFirst());
    }
}



