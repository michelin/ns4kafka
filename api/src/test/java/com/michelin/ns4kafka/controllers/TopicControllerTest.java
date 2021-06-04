package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.TopicService;
import com.michelin.ns4kafka.validation.TopicValidator;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TopicControllerTest {

    @Mock
    NamespaceService namespaceService;
    @Mock
    TopicService topicService;

    @InjectMocks
    TopicController topicController;

    @Test
    public void ListEmptyTopics() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of());

        List<Topic> actual = topicController.list("test");
        Assertions.assertEquals(0, actual.size());
    }

    @Test
    public void ListMultipleTopics() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Topic.builder().metadata(ObjectMeta.builder().name("topic1").build()).build(),
                        Topic.builder().metadata(ObjectMeta.builder().name("topic2").build()).build()
                ));

        List<Topic> actual = topicController.list("test");

        Assertions.assertEquals(2, actual.size());
        Assertions.assertEquals("topic1", actual.get(0).getMetadata().getName());
        Assertions.assertEquals("topic2", actual.get(1).getMetadata().getName());
    }

    @Test
    public void GetEmptyTopic() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "topic.notfound"))
                .thenReturn(Optional.empty());

        Optional<Topic> actual = topicController.getTopic("test", "topic.notfound");

        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    public void GetTopic() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "topic.found"))
                .thenReturn(Optional.of(
                        Topic.builder().metadata(ObjectMeta.builder().name("topic.found").build()).build()
                ));

        Optional<Topic> actual = topicController.getTopic("test", "topic.found");

        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("topic.found", actual.get().getMetadata().getName());
    }

    @Test
    public void DeleteTopic() throws InterruptedException, ExecutionException, TimeoutException {
        //Given
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Optional<Topic> toDelete = Optional.of(
                Topic.builder().metadata(ObjectMeta.builder().name("topic.delete").build()).build());
        when(topicService.findByName(ns, "topic.delete"))
                .thenReturn(toDelete);
        when(topicService.isNamespaceOwnerOfTopic("test","topic.delete"))
                .thenReturn(true);
        doNothing().when(topicService).delete(toDelete.get());


        //When
        HttpResponse<Void> actual = topicController.deleteTopic("test", "topic.delete", false);

        //Then
        Assertions.assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
    }

    @Test
    public void DeleteTopicDryRun() throws InterruptedException, ExecutionException, TimeoutException {
        //Given
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Optional<Topic> toDelete = Optional.of(
                Topic.builder().metadata(ObjectMeta.builder().name("topic.delete").build()).build());
        when(topicService.findByName(ns, "topic.delete"))
                .thenReturn(toDelete);
        when(topicService.isNamespaceOwnerOfTopic("test","topic.delete"))
                .thenReturn(true);

        //When
        HttpResponse<Void> actual = topicController.deleteTopic("test", "topic.delete", true);

        //Then
        verify(topicService, never()).delete(any());
    }

    @Test
    public void DeleteTopicUnauthorized() throws InterruptedException, ExecutionException, TimeoutException {
        //Given
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.delete"))
                .thenReturn(false);

        //When
        HttpResponse<Void> actual = topicController.deleteTopic("test", "topic.delete", false);

        //Then
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, actual.getStatus());

    }

    @Test
    public void CreateNewTopic() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
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
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(topicService.create(topic)).thenReturn(topic);

        Topic actual = topicController.apply("test", topic, false);
        assertEquals(actual.getMetadata().getName(), "test.topic");
    }

    @Test
    public void UpdateTopic() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
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
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.of(existing));
        when(topicService.create(topic)).thenReturn(topic);

        Topic actual = topicController.apply("test", topic, false);
        assertEquals(actual.getMetadata().getName(), "test.topic");
    }

    @Test
    public void UpdateTopic_AlreadyExists() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
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
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","compact",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.of(existing));

        Topic actual = topicController.apply("test", topic, false);
        verify(topicService, never()).create(ArgumentMatchers.any());
        assertEquals(existing, actual);

    }

    @Test
    public void CreateNewTopicDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
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
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());

        Topic actual = topicController.apply("test", topic, true);
        verify(topicService, never()).create(topic);
    }

    @Test
    public void CreateNewTopicFailValidationNoAPI() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();
        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(1)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                        "min.insync.replicas", "2",
                                        "retention.ms", "60000"))
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> topicController.apply("test", topic, false));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertLinesMatch(List.of(".*replication\\.factor.*"), actual.getValidationErrors());
    }

    @Test
    public void ImportTopic() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();
        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic1")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();
        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic2")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();
        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic3")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.listUnsynchronizedTopics(ns))
                .thenReturn(List.of(topic1, topic2));
        when(topicService.create(topic1)).thenReturn(topic1);
        when(topicService.create(topic2)).thenReturn(topic2);


        List<Topic> actual = topicController.importResources("test", false);
        Assertions.assertTrue(actual.stream()
                .anyMatch(t -> 
                        t.getMetadata().getName().equals("test.topic1")
                        && t.getStatus().getMessage().equals("Imported from cluster")
                        && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)
        ));
        Assertions.assertTrue(actual.stream()
                .anyMatch(t ->
                        t.getMetadata().getName().equals("test.topic2")
                                && t.getStatus().getMessage().equals("Imported from cluster")
                                && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)
                ));
        Assertions.assertFalse(actual.stream()
                .anyMatch(t ->
                        t.getMetadata().getName().equals("test.topic3")
                ));
    }

    @Test
    public void ImportTopicDryRun() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();
        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic1")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();
        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic2")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();
        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.topic3")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.listUnsynchronizedTopics(ns))
                .thenReturn(List.of(topic1, topic2));


        List<Topic> actual = topicController.importResources("test", true);
        Assertions.assertTrue(actual.stream()
                .anyMatch(t ->
                        t.getMetadata().getName().equals("test.topic1")
                                && t.getStatus().getMessage().equals("Imported from cluster")
                                && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)
                ));
        Assertions.assertTrue(actual.stream()
                .anyMatch(t ->
                        t.getMetadata().getName().equals("test.topic2")
                                && t.getStatus().getMessage().equals("Imported from cluster")
                                && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)
                ));
        Assertions.assertFalse(actual.stream()
                .anyMatch(t ->
                        t.getMetadata().getName().equals("test.topic3")
                ));
    }

}
