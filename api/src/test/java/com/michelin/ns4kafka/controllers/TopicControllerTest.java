package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.DeleteRecordsResponse;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import com.michelin.ns4kafka.services.TopicService;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import org.apache.kafka.common.TopicPartition;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TopicControllerTest {
    /**
     * The mocked namespace service
     */
    @Mock
    NamespaceService namespaceService;

    /**
     * The mocked topic service
     */
    @Mock
    TopicService topicService;

    /**
     * The mocked app event publisher
     */
    @Mock
    ApplicationEventPublisher applicationEventPublisher;

    /**
     * The mocked security service
     */
    @Mock
    SecurityService securityService;

    /**
     * The mocked resource quota service
     */
    @Mock
    ResourceQuotaService resourceQuotaService;

    /**
     * The mocked topic controller
     */
    @InjectMocks
    TopicController topicController;

    /**
     * Validate empty topics listing
     */
    @Test
    void listEmptyTopics() {
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

    /**
     * Validate topics listing
     */
    @Test
    void listMultipleTopics() {
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

    /**
     * Validate get topic empty response
     */
    @Test
    void getEmptyTopic() {
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

    /**
     * Validate get topic
     */
    @Test
    void getTopic() {
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

    /**
     * Validate topic deletion
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void deleteTopic() throws InterruptedException, ExecutionException, TimeoutException {
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
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(topicService).delete(toDelete.get());
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> actual = topicController.deleteTopic("test", "topic.delete", false);

        Assertions.assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
    }

    /**
     * Validate topic deletion in dry mode
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void deleteTopicDryRun() throws InterruptedException, ExecutionException, TimeoutException {
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

        topicController.deleteTopic("test", "topic.delete", true);

        verify(topicService, never()).delete(any());
    }

    /**
     * Validate topic deletion when unauthorized
     */
    @Test
    void deleteTopicUnauthorized()  {
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

        Assertions.assertThrows(ResourceValidationException.class,
        () -> topicController.deleteTopic("test", "topic.delete", false));
    }

    /**
     * Validate topic creation
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void createNewTopic() throws InterruptedException, ExecutionException, TimeoutException {
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
        when(resourceQuotaService.validateTopicQuota(ns, topic)).thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        when(topicService.create(topic)).thenReturn(topic);

        var response = topicController.apply("test", topic, false);
        Topic actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test.topic", actual.getMetadata().getName());
    }

    /**
     * Validate topic update
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void updateTopic() throws InterruptedException, ExecutionException, TimeoutException {
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
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        var response = topicController.apply("test", topic, false);
        Topic actual = response.body();
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("test.topic", actual.getMetadata().getName());
    }

    /**
     * Validate topic update when there are validations errors
     */
    @Test
    void updateTopicValidationErrors() {
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
                        .partitions(6)
                        .configs(Map.of("cleanup.policy","delete",
                                "min.insync.replicas", "2",
                                "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.of(existing));
        when(topicService.validateTopicUpdate(ns, existing, topic)).thenReturn(List.of("Invalid value 6 for configuration partitions: Value is immutable (3)."));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> topicController.apply("test", topic, false));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertLinesMatch(List.of("Invalid value 6 for configuration partitions: Value is immutable (3)."), actual.getValidationErrors());
    }

    /**
     * Validate topic update when topic doesn't change
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void updateTopicAlreadyExistsUnchanged() throws InterruptedException, ExecutionException, TimeoutException {
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

        var response = topicController.apply("test", topic, false);
        Topic actual = response.body();
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(ArgumentMatchers.any());
        assertEquals(existing, actual);
    }

    /**
     * Validate topic creation in dry mode
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void createNewTopicDryRun() throws InterruptedException, ExecutionException, TimeoutException {
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
        when(resourceQuotaService.validateTopicQuota(ns, topic)).thenReturn(List.of());

        var response = topicController.apply("test", topic, true);
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(topic);
    }

    /**
     * Validate topic creation when topic validation fails
     */
    @Test
    void createNewTopicFailValidation() {
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

    /**
     * Validate topic creation when topic quota validation fails
     */
    @Test
    void createNewTopicFailQuotaValidation() {
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
        when(resourceQuotaService.validateTopicQuota(ns, topic)).thenReturn(List.of("Quota error"));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> topicController.apply("test", topic, false));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertLinesMatch(List.of("Quota error"), actual.getValidationErrors());
    }

    /**
     * Validate topic import
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void importTopic() throws InterruptedException, ExecutionException, TimeoutException {
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

    /**
     * Validate topic import in dry mode
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void importTopicDryRun() throws InterruptedException, ExecutionException, TimeoutException {
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

    /**
     * Validate delete records
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     */
    @Test
    void deleteRecordsSuccess() throws ExecutionException, InterruptedException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Topic toEmpty = Topic.builder().metadata(ObjectMeta.builder().name("topic.empty").build()).build();

        Map<TopicPartition, Long> partitionsToDelete = Map.of(
                new TopicPartition("topic.empty",0), 100L,
                new TopicPartition("topic.empty", 1), 101L);

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test","topic.empty"))
                .thenReturn(true);
        when(topicService.validateDeleteRecordsTopic(toEmpty))
                .thenReturn(List.of());
        when(topicService.findByName(ns, "topic.empty"))
                .thenReturn(Optional.of(toEmpty));
        when(topicService.prepareRecordsToDelete(toEmpty))
                .thenReturn(partitionsToDelete);
        when(topicService.deleteRecords(ArgumentMatchers.eq(toEmpty), anyMap()))
                .thenReturn(partitionsToDelete);

        List<DeleteRecordsResponse> actual = topicController.deleteRecords("test", "topic.empty", false);

        DeleteRecordsResponse resultPartition0 = actual
                .stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 0)
                .findFirst()
                .orElse(null);

        DeleteRecordsResponse resultPartition1 = actual
                .stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 1)
                .findFirst()
                .orElse(null);

        Assertions.assertEquals(2L, actual.size());

        assertNotNull(resultPartition0);
        Assertions.assertEquals(100L, resultPartition0.getSpec().getOffset());
        Assertions.assertEquals(0, resultPartition0.getSpec().getPartition());
        Assertions.assertEquals("topic.empty", resultPartition0.getSpec().getTopic());

        assertNotNull(resultPartition1);
        Assertions.assertEquals(101L, resultPartition1.getSpec().getOffset());
        Assertions.assertEquals(1, resultPartition1.getSpec().getPartition());
        Assertions.assertEquals("topic.empty", resultPartition1.getSpec().getTopic());
    }

    /**
     * Validate delete records fails on compacted topic
     */
    @Test
    void deleteRecordsCompactedTopic() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Topic toEmpty = Topic.builder().metadata(ObjectMeta.builder().name("topic.empty").build()).build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test","topic.empty"))
                .thenReturn(true);
        when(topicService.validateDeleteRecordsTopic(toEmpty))
                .thenReturn(List.of("Cannot delete records on a compacted topic. Please delete and recreate the topic."));
        when(topicService.findByName(ns, "topic.empty"))
                .thenReturn(Optional.of(toEmpty));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> topicController.deleteRecords("test", "topic.empty", false));

        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertLinesMatch(List.of("Cannot delete records on a compacted topic. Please delete and recreate the topic."),
                actual.getValidationErrors());
    }

    /**
     * Validate delete records in dry mode
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     */
    @Test
    void deleteRecordsDryRun() throws InterruptedException, ExecutionException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Topic toEmpty = Topic.builder().metadata(ObjectMeta.builder().name("topic.empty").build()).build();

        Map<TopicPartition, Long> partitionsToDelete = Map.of(
                new TopicPartition("topic.empty",0), 100L,
                new TopicPartition("topic.empty", 1), 101L);

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test","topic.empty"))
                .thenReturn(true);
        when(topicService.validateDeleteRecordsTopic(toEmpty))
                .thenReturn(List.of());
        when(topicService.findByName(ns, "topic.empty"))
                .thenReturn(Optional.of(toEmpty));
        when(topicService.prepareRecordsToDelete(toEmpty))
                .thenReturn(partitionsToDelete);

        List<DeleteRecordsResponse> actual = topicController.deleteRecords("test", "topic.empty", true);

        DeleteRecordsResponse resultPartition0 = actual
                .stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 0)
                .findFirst()
                .orElse(null);

        DeleteRecordsResponse resultPartition1 = actual
                .stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 1)
                .findFirst()
                .orElse(null);

        Assertions.assertEquals(2L, actual.size());

        assertNotNull(resultPartition0);
        Assertions.assertEquals(100L, resultPartition0.getSpec().getOffset());
        Assertions.assertEquals(0, resultPartition0.getSpec().getPartition());
        Assertions.assertEquals("topic.empty", resultPartition0.getSpec().getTopic());

        assertNotNull(resultPartition1);
        Assertions.assertEquals(101L, resultPartition1.getSpec().getOffset());
        Assertions.assertEquals(1, resultPartition1.getSpec().getPartition());
        Assertions.assertEquals("topic.empty", resultPartition1.getSpec().getTopic());

        verify(topicService, never()).deleteRecords(any(), anyMap());
    }

    /**
     * Validate delete records when not owner of topic
     */
    @Test
    void deleteRecordsNotOwner() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test","topic.empty"))
                .thenReturn(false);

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> topicController.deleteRecords("test", "topic.empty", false));

        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertLinesMatch(List.of("Namespace not owner of this topic \"topic.empty\"."),
                actual.getValidationErrors());
    }

    /**
     * Validate delete records when not owner of topic
     */
    @Test
    void deleteRecordsNotExistingTopic() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test","topic.empty"))
                .thenReturn(true);
        when(topicService.findByName(ns, "topic.empty"))
                .thenReturn(Optional.empty());

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> topicController.deleteRecords("test", "topic.empty", false));

        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertLinesMatch(List.of("Topic \"topic.empty\" does not exist."),
                actual.getValidationErrors());
    }

    /**
     * Validate topic creation with name collision
     * @throws InterruptedException Any interrupted exception
     * @throws ExecutionException Any execution exception
     * @throws TimeoutException Any timeout exception
     */
    @Test
    void createCollidingTopic() throws InterruptedException, ExecutionException, TimeoutException {
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
        when(topicService.findCollidingTopics(ns, topic)).thenReturn(List.of("test_topic"));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class, () -> topicController.apply("test", topic, false));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertLinesMatch(
                List.of("Topic test.topic collides with existing topics: test_topic."),
                actual.getValidationErrors());
    }
}
