package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.TagInfo;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopicServiceTest {
    @InjectMocks
    TopicService topicService;

    @Mock
    AccessControlEntryService accessControlEntryService;

    @Mock
    TopicRepository topicRepository;

    @Mock
    ApplicationContext applicationContext;

    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    /**
     * Validate find topic by name
     */
    @Test
    void findByName() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic t1 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns1-topic1").build())
                .build();

        Topic t4 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns2-topic1").build())
                .build();

        when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of(t1, t2, t3, t4));

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
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

    /**
     * Validate empty response when no topic in namespace
     */
    @Test
    void findAllForNamespaceNoTopics() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // no ns4kfk access control entries
        when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of());

        // no ns4kfk topics 
        when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        // get list of topics
        List<Topic> list = topicService.findAllForNamespace(ns);

        // list of topics is empty
        assertTrue(list.isEmpty());
    }

    /**
     * Validate empty response when no topic ACLs
     */
    @Test
    void findAllForNamespaceNoAcls() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // init ns4kfk topics
        Topic t1 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .build();
        Topic t2 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic2").build())
                .build();
        Topic t3 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns1-topic1").build())
                .build();
        Topic t4 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns2-topic1").build())
                .build();
        when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of(t1, t2, t3, t4));

        // no ns4kfk access control entries
        when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of());

        // list of topics is empty 
        List<Topic> actual = topicService.findAllForNamespace(ns);
        assertTrue(actual.isEmpty());
    }

    /**
     * Validate find all topics for namespace
     */
    @Test
    void findAllForNamespace() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // init ns4kfk topics
        Topic t0 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns0-topic1").build())
                .build();
        Topic t1 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .build();
        Topic t2 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic2").build())
                .build();
        Topic t3 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns1-topic1").build())
                .build();
        Topic t4 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns2-topic1").build())
                .build();
        when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of(t0,t1, t2, t3, t4));

        // ns4kfk access control entries
        when(accessControlEntryService.findAllGrantedToNamespace(ns))
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

    /**
     * Validate unsynchronized topics listing
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void listUnsynchronizedNoExistingTopics() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
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
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic1"))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic2"))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns1-topic1"))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns2-topic1"))
                .thenReturn(false);

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
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

    /**
     * Validate unsynchronized topics listing when all topics existing
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void listUnsynchronizedAllExistingTopics() throws InterruptedException, ExecutionException, TimeoutException {
        // init ns4kfk namespace
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // init ns4kfk topics
        Topic t1 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .build();
        Topic t2 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic2").build())
                .build();
        Topic t3 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns1-topic1").build())
                .build();
        Topic t4 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns2-topic1").build())
                .build();

        // init topicAsyncExecutor
        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of(t1.getMetadata().getName(), t2.getMetadata().getName(),
                t3.getMetadata().getName(), t4.getMetadata().getName()));

        // list of existing ns4kfk access control entries
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, t1.getMetadata().getName()))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, t2.getMetadata().getName()))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, t3.getMetadata().getName()))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, t4.getMetadata().getName()))
                .thenReturn(false);

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
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

    /**
     * Validate unsynchronized topics listing when some topics existing and some not
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void listUnsynchronizedPartialExistingTopics() throws InterruptedException, ExecutionException, TimeoutException {
        // init ns4kfk namespace
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // init ns4kfk topics
        Topic t1 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .build();

        // init topicAsyncExecutor
        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2",
                "ns1-topic1", "ns2-topic1"));

        // list of existing ns4kfk access control entries
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic1"))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic2"))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns1-topic1"))
                .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns2-topic1"))
                .thenReturn(false);

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
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

    /**
     * Validate colliding topics when there is no collision
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void findCollidingTopicsNoCollision() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("project1.topic").build())
                .build();

        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of("project2.topic", "project1.other"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        assertTrue(actual.isEmpty());
    }

    /**
     * Validate colliding topics when names collide
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void findCollidingTopicsIdenticalName() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("project1.topic").build())
                .build();

        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of("project1.topic", "project2.topic", "project1.other"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        assertTrue(actual.isEmpty(), "Topic with exactly the same name should not interfere with collision check");
    }

    /**
     * Validate colliding topics when names collide
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void findCollidingTopicsCollidingName() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("project1.topic").build())
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

    /**
     * Validate colliding topics when there is an interrupted exception
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void findCollidingTopicsInterruptedException() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("project1.topic").build())
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

    /**
     * Validate colliding topics when there is a runtime exception
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void findCollidingTopicsOtherException() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("project1.topic").build())
                .build();

        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        when(topicAsyncExecutor.listBrokerTopicNames())
                .thenThrow(new RuntimeException("Unknown Error"));

        assertThrows(RuntimeException.class,
                () -> topicService.findCollidingTopics(ns, topic));
    }

    /**
     * Validate a topic is eligible for record deletion
     */
    @Test
    void validateDeleteRecordsTopic() {
        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("project1.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Collections.singletonMap("cleanup.policy", "compact"))
                        .build())
                .build();

        List<String> actual = topicService.validateDeleteRecordsTopic(topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Cannot delete records on a compacted topic. Please delete and recreate the topic."), actual);
    }

    /**
     * Validate topic update when partition number change
     */
    @Test
    void validateTopicUpdatePartitions() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic existing = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .namespace("test")
                        .cluster("local")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","compact",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(6)
                        .configs(Map.of("cleanup.policy","compact",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of());

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Invalid value 6 for configuration partitions: Value is immutable (3)."), actual);
    }

    /**
     * Validate topic update when replication factor change
     */
    @Test
    void validateTopicUpdateReplicationFactor() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic existing = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","compact",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(6)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","compact",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of());

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Invalid value 6 for configuration replication.factor: Value is immutable (3)."), actual);
    }

    /**
     * Validate topic update when cleanup policy change from delete to compact on Confluent Cloud
     */
    @Test
    void validateTopicUpdateCleanupPolicyDeleteToCompactOnCCloud() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic existing = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","compact",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of(new KafkaAsyncExecutorConfig("local", KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("Invalid value compact for configuration cleanup.policy: Altering topic configuration from `delete` to `compact` is not currently supported. Please create a new topic with `compact` policy specified instead."), actual);
    }

    /**
     * Validate topic update when cleanup policy change from compact to delete on Confluent Cloud
     */
    @Test
    void validateTopicUpdateCleanupPolicyCompactToDeleteOnCCloud() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic existing = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","compact",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of(new KafkaAsyncExecutorConfig("local", KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(0, actual.size());
    }

    /**
     * Validate topic update when cleanup policy change from delete to compact on Confluent Cloud
     */
    @Test
    void validateTopicUpdateCleanupPolicyDeleteToCompactOnSelfManaged() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic existing = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","compact",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of(new KafkaAsyncExecutorConfig("local", KafkaAsyncExecutorConfig.KafkaProvider.SELF_MANAGED)));

        List<String> actual = topicService.validateTopicUpdate(ns, existing, topic);

        assertEquals(0, actual.size());
    }

    /**
     * Validate find all for all namespaces
     */
    @Test
    void findAll() {
        Topic t1 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .build();

        Topic t2 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic2").build())
                .build();

        Topic t3 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns1-topic1").build())
                .build();

        Topic t4 = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns2-topic1").build())
                .build();

        when(topicRepository.findAll()).thenReturn(List.of(t1, t2, t3, t4));

        List<Topic> topics = topicService.findAll();
        assertEquals(4, topics.size());
    }

    @Test
    void shouldTagsBeValid() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .spec(Topic.TopicSpec.builder()
                    .tags(List.of("TAG_TEST")).build())
                .build();

        List<TagInfo> tagInfo = List.of(TagInfo.builder().name("TAG_TEST").build());

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of(new KafkaAsyncExecutorConfig("local", KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD)));
        when(schemaRegistryClient.getTags("local")).thenReturn(Mono.just(tagInfo));

        List<String> validationErrors = topicService.validateTags(ns, topic);
        assertEquals(0, validationErrors.size());
    }

    @Test
    void shouldTagsBeInvalidWhenNotConfluentCloud() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of("TAG_TEST")).build())
                .build();

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of(new KafkaAsyncExecutorConfig("local", KafkaAsyncExecutorConfig.KafkaProvider.SELF_MANAGED)));

        List<String> validationErrors = topicService.validateTags(ns, topic);
        assertEquals(1, validationErrors.size());
        assertEquals("Tags can only be used on confluent clusters.", validationErrors.get(0));
    }

    @Test
    void shouldTagsBeInvalidWhenNoTagsAllowed() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of("TAG_TEST")).build())
                .build();

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of(new KafkaAsyncExecutorConfig("local", KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD)));
        when(schemaRegistryClient.getTags("local")).thenReturn(Mono.just(Collections.emptyList()));

        List<String> validationErrors = topicService.validateTags(ns, topic);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value (TAG_TEST) for tags: No tags defined on the kafka cluster.", validationErrors.get(0));
    }

    @Test
    void shouldTagsBeInvalidWhenNotAllowed() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("ns-topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of("TAG_TEST")).build())
                .build();

        List<TagInfo> tagInfo = List.of(TagInfo.builder().name("TAG_TEST").build());

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of(new KafkaAsyncExecutorConfig("local", KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD)));
        when(schemaRegistryClient.getTags("local")).thenReturn(Mono.just(tagInfo));

        List<String> validationErrors = topicService.validateTags(ns, topic);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value (BAD_TAG) for tags: Available tags are (TAG_TEST).", validationErrors.get(0));
    }
}
