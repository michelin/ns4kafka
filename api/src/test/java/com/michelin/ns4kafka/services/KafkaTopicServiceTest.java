package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
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

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@ExtendWith(MockitoExtension.class)
public class KafkaTopicServiceTest {

    @InjectMocks
    TopicService topicService;

    @Mock
    AccessControlEntryService accessControlEntryService;

    @Mock
    TopicRepository topicRepository;

    @Mock
    ApplicationContext applicationContext;

    @Test
    void findByName() {

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
        Mockito.when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of(t1, t2, t3, t4));

        // ns4kfk access control entries
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
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
        Assertions.assertEquals(actualTopicPrefixed.get(), t1);

        Optional<Topic> actualTopicLiteral = topicService.findByName(ns, "ns1-topic1");
        Assertions.assertEquals(actualTopicLiteral.get(), t3);

        Optional<Topic> actualTopicNotFound = topicService.findByName(ns, "ns2-topic1");
        Assertions.assertThrows(NoSuchElementException.class, () -> actualTopicNotFound.get(), "No value present");
    }


    @Test
    void findAllForNamespaceNoTopics() {

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

        // no ns4kfk access control entries
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of()
                );

        // no ns4kfk topics 
        Mockito.when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        // get list of topics
        List<Topic> list = topicService.findAllForNamespace(ns);

        // list of topics is empty
        Assertions.assertTrue(list.isEmpty());
    }

    @Test
    void findAllForNamespaceNoAcls() {

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
        Mockito.when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of(t1, t2, t3, t4));

        // no ns4kfk access control entries
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of()
                );

        // list of topics is empty 
        List<Topic> actual = topicService.findAllForNamespace(ns);
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void findAllForNamespace() {

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
        Mockito.when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of(t0,t1, t2, t3, t4));

        // ns4kfk access control entries
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
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

        Assertions.assertEquals(3, actual.size());
        // contains
        Assertions.assertTrue(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns0-topic1")));
        Assertions.assertTrue(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns-topic1")));
        Assertions.assertTrue(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns-topic2")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns1-topic1")));
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.getMetadata().getName().equals("ns2-topic1")));
    }

    @Test
    void listUnsynchronizedNoExistingTopics() throws InterruptedException, ExecutionException, TimeoutException {

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

        // init topicAsyncExecutor
        TopicAsyncExecutor topicAsyncExecutor = Mockito.mock(TopicAsyncExecutor.class);
        Mockito.when(applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        Mockito.when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2",
                "ns1-topic1", "ns2-topic1"));

        // list of existing ns4kfk access control entries
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic2"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns1-topic1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns2-topic1"))
                .thenReturn(false);

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
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
        Mockito.when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of());
        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        Assertions.assertEquals(3, actual.size());
        // contains
        Assertions.assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic1")));
        Assertions.assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic2")));
        Assertions.assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns1-topic1")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns2-topic1")));

    }

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
        Mockito.when(applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        Mockito.when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of(t1.getMetadata().getName(), t2.getMetadata().getName(),
                t3.getMetadata().getName(), t4.getMetadata().getName()));

        // list of existing ns4kfk access control entries
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, t1.getMetadata().getName()))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, t2.getMetadata().getName()))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, t3.getMetadata().getName()))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, t4.getMetadata().getName()))
                .thenReturn(false);

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
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
        Mockito.when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of(t1, t2, t3, t4));

        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        Assertions.assertEquals(0, actual.size());

    }

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
        Mockito.when(applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(topicAsyncExecutor);

        // list of existing broker topics
        Mockito.when(topicAsyncExecutor.listBrokerTopicNames()).thenReturn(List.of("ns-topic1", "ns-topic2",
                "ns1-topic1", "ns2-topic1"));

        // list of existing ns4kfk access control entries
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns-topic2"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns1-topic1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.TOPIC, "ns2-topic1"))
                .thenReturn(false);

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
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
        Mockito.when(topicRepository.findAllForCluster("local"))
                .thenReturn(List.of(t1));

        List<String> actual = topicService.listUnsynchronizedTopicNames(ns);

        Assertions.assertEquals(2, actual.size());
        // contains
        Assertions.assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns-topic2")));
        Assertions.assertTrue(actual.stream().anyMatch(topic -> topic.equals("ns1-topic1")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns-topic1")));
        Assertions.assertFalse(actual.stream().anyMatch(topic -> topic.equals("ns2-topic1")));

    }

    @Test
    void testFindCollidingTopics_NoCollision() throws ExecutionException, InterruptedException, TimeoutException {
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
        Mockito.when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        Mockito.when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of("project2.topic", "project1.other"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void testFindCollidingTopics_IdenticalName() throws ExecutionException, InterruptedException, TimeoutException {
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
        Mockito.when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        Mockito.when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of("project1.topic", "project2.topic", "project1.other"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        Assertions.assertTrue(actual.isEmpty(), "Topic with exactly the same name should not interfere with collision check");
    }

    @Test
    void testFindCollidingTopics_CollidingName() throws ExecutionException, InterruptedException, TimeoutException {
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
        Mockito.when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        Mockito.when(topicAsyncExecutor.listBrokerTopicNames())
                .thenReturn(List.of("project1_topic"));

        List<String> actual = topicService.findCollidingTopics(ns, topic);

        Assertions.assertEquals(1, actual.size());
        Assertions.assertLinesMatch(List.of("project1_topic"), actual);
    }

    @Test
    void testFindCollidingTopics_InterruptedException() throws ExecutionException, InterruptedException, TimeoutException {
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
        Mockito.when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        Mockito.when(topicAsyncExecutor.listBrokerTopicNames())
                .thenThrow(new InterruptedException());

        var actual = Assertions.assertThrows(InterruptedException.class,
                () -> topicService.findCollidingTopics(ns, topic));

        Assertions.assertTrue(Thread.interrupted());
        //Assertions.assertEquals(1, actual.getValidationErrors().size());
    }

    @Test
    void testFindCollidingTopics_OtherException() throws ExecutionException, InterruptedException, TimeoutException {
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
        Mockito.when(applicationContext.getBean(TopicAsyncExecutor.class, Qualifiers.byName("local")))
                .thenReturn(topicAsyncExecutor);
        Mockito.when(topicAsyncExecutor.listBrokerTopicNames())
                .thenThrow(new RuntimeException("Unknown Error"));

        var actual = Assertions.assertThrows(RuntimeException.class,
                () -> topicService.findCollidingTopics(ns, topic));

        //Assertions.assertEquals(1, actual.getValidationErrors().size());
    }

}
