package com.michelin.ns4kafka.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controllers.topic.TopicController;
import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.DeleteRecordsResponse;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import com.michelin.ns4kafka.services.TopicService;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    TopicService topicService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @Mock
    ResourceQuotaService resourceQuotaService;

    @InjectMocks
    TopicController topicController;

    @Test
    void listEmptyTopics() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of());

        List<Topic> actual = topicController.list("test");
        assertEquals(0, actual.size());
    }

    @Test
    void listMultipleTopics() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(
                Topic.builder().metadata(ObjectMeta.builder().name("topic1").build()).build(),
                Topic.builder().metadata(ObjectMeta.builder().name("topic2").build()).build()
            ));

        List<Topic> actual = topicController.list("test");

        assertEquals(2, actual.size());
        assertEquals("topic1", actual.get(0).getMetadata().getName());
        assertEquals("topic2", actual.get(1).getMetadata().getName());
    }

    @Test
    void getEmptyTopic() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "topic.notfound"))
            .thenReturn(Optional.empty());

        Optional<Topic> actual = topicController.getTopic("test", "topic.notfound");

        assertTrue(actual.isEmpty());
    }

    @Test
    void getTopic() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "topic.found"))
            .thenReturn(Optional.of(
                Topic.builder().metadata(ObjectMeta.builder().name("topic.found").build()).build()
            ));

        Optional<Topic> actual = topicController.getTopic("test", "topic.found");

        assertTrue(actual.isPresent());
        assertEquals("topic.found", actual.get().getMetadata().getName());
    }

    @Test
    void deleteTopic() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        Optional<Topic> toDelete = Optional.of(
            Topic.builder().metadata(ObjectMeta.builder().name("topic.delete").build()).build());
        when(topicService.findByName(ns, "topic.delete"))
            .thenReturn(toDelete);
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.delete"))
            .thenReturn(true);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(topicService).delete(toDelete.get());
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> actual = topicController.deleteTopic("test", "topic.delete", false);

        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
    }

    @Test
    void deleteTopicDryRun() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        Optional<Topic> toDelete = Optional.of(
            Topic.builder().metadata(ObjectMeta.builder().name("topic.delete").build()).build());
        when(topicService.findByName(ns, "topic.delete"))
            .thenReturn(toDelete);
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.delete"))
            .thenReturn(true);

        topicController.deleteTopic("test", "topic.delete", true);

        verify(topicService, never()).delete(any());
    }

    @Test
    void deleteTopicUnauthorized() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.delete"))
            .thenReturn(false);

        assertThrows(ResourceValidationException.class,
            () -> topicController.deleteTopic("test", "topic.delete", false));
    }

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
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(resourceQuotaService.validateTopicQuota(ns, Optional.empty(), topic)).thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        when(topicService.create(topic)).thenReturn(topic);

        var response = topicController.apply("test", topic, false);
        Topic actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test.topic", actual.getMetadata().getName());
    }

    @Test
    void shouldCreateNewTopicWithNoConstraint() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .topicValidator(TopicValidator.builder()
                    .build())
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
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

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(resourceQuotaService.validateTopicQuota(ns, Optional.empty(), topic)).thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        when(topicService.create(topic)).thenReturn(topic);

        var response = topicController.apply("test", topic, false);
        Topic actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test.topic", actual.getMetadata().getName());
    }

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
                .configs(Map.of("cleanup.policy", "compact",
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
                .configs(Map.of("cleanup.policy", "delete",
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
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("test.topic", actual.getMetadata().getName());
    }

    @Test
    void shouldValidateNewTags() {
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
                .tags(Arrays.asList("TAG1", "TAG3"))
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .tags(Arrays.asList("TAG1", "TAG2"))
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.of(existing));
        when(topicService.validateTags(ns, topic)).thenReturn(List.of("Error on tags"));

        ResourceValidationException actual =
            assertThrows(ResourceValidationException.class, () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Error on tags"), actual.getValidationErrors());
    }
    
    @Test
    void shouldNotValidateTagsWhenNoNewTag() throws InterruptedException, ExecutionException, TimeoutException {
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
                .tags(Arrays.asList("TAG1", "TAG2"))
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name("test.topic")
                .build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of("TAG1"))
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
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
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("test.topic", actual.getMetadata().getName());
        assertEquals(1, actual.getSpec().getTags().size());
        assertEquals("TAG1", actual.getSpec().getTags().get(0));
    }

    /**
     * Validate topic update when there are validations errors.
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
                .configs(Map.of("cleanup.policy", "compact",
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
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.of(existing));
        when(topicService.validateTopicUpdate(ns, existing, topic)).thenReturn(
            List.of("Invalid value 6 for configuration partitions: Value is immutable (3)."));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Invalid value 6 for configuration partitions: Value is immutable (3)."),
            actual.getValidationErrors());
    }

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
                .configs(Map.of("cleanup.policy", "compact",
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
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.of(existing));

        var response = topicController.apply("test", topic, false);
        Topic actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(ArgumentMatchers.any());
        assertEquals(existing, actual);
    }

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
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(resourceQuotaService.validateTopicQuota(ns, Optional.empty(), topic)).thenReturn(List.of());

        var response = topicController.apply("test", topic, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(topic);
    }

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
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of(".*replication\\.factor.*"), actual.getValidationErrors());
    }

    @Test
    void shouldNotFailWhenCreatingNewTopicWithNoValidator()
        throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .topicValidator(null)
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder().name("test.topic").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(1)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());

        var response = topicController.apply("test", topic, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(topic);
    }

    @Test
    void shouldNotFailWhenCreatingNewTopicWithNoValidationConstraint()
        throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .topicValidator(TopicValidator.builder()
                    .build())
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder().name("test.topic").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(1)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());

        var response = topicController.apply("test", topic, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(topic);
    }

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
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(resourceQuotaService.validateTopicQuota(ns, Optional.empty(), topic)).thenReturn(List.of("Quota error"));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Quota error"), actual.getValidationErrors());
    }

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
                .configs(Map.of("cleanup.policy", "delete",
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
                .configs(Map.of("cleanup.policy", "delete",
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

        assertTrue(actual.stream()
            .anyMatch(t ->
                t.getMetadata().getName().equals("test.topic1")
                    && t.getStatus().getMessage().equals("Imported from cluster")
                    && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)
            ));

        assertTrue(actual.stream()
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
                .configs(Map.of("cleanup.policy", "delete",
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
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.listUnsynchronizedTopics(ns))
            .thenReturn(List.of(topic1, topic2));

        List<Topic> actual = topicController.importResources("test", true);

        assertTrue(actual.stream()
            .anyMatch(t ->
                t.getMetadata().getName().equals("test.topic1")
                    && t.getStatus().getMessage().equals("Imported from cluster")
                    && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)
            ));

        assertTrue(actual.stream()
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
    void deleteRecordsSuccess() throws ExecutionException, InterruptedException {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        Topic toEmpty = Topic.builder().metadata(ObjectMeta.builder().name("topic.empty").build()).build();

        Map<TopicPartition, Long> partitionsToDelete = Map.of(
            new TopicPartition("topic.empty", 0), 100L,
            new TopicPartition("topic.empty", 1), 101L);

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty"))
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

        assertEquals(2L, actual.size());

        assertNotNull(resultPartition0);
        assertEquals(100L, resultPartition0.getSpec().getOffset());
        assertEquals(0, resultPartition0.getSpec().getPartition());
        assertEquals("topic.empty", resultPartition0.getSpec().getTopic());

        DeleteRecordsResponse resultPartition1 = actual
            .stream()
            .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 1)
            .findFirst()
            .orElse(null);

        assertNotNull(resultPartition1);
        assertEquals(101L, resultPartition1.getSpec().getOffset());
        assertEquals(1, resultPartition1.getSpec().getPartition());
        assertEquals("topic.empty", resultPartition1.getSpec().getTopic());
    }

    @Test
    void deleteRecordsCompactedTopic() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        Topic toEmpty = Topic.builder().metadata(ObjectMeta.builder().name("topic.empty").build()).build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty"))
            .thenReturn(true);
        when(topicService.validateDeleteRecordsTopic(toEmpty))
            .thenReturn(List.of("Cannot delete records on a compacted topic. Please delete and recreate the topic."));
        when(topicService.findByName(ns, "topic.empty"))
            .thenReturn(Optional.of(toEmpty));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> topicController.deleteRecords("test", "topic.empty", false));

        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Cannot delete records on a compacted topic. Please delete and recreate the topic."),
            actual.getValidationErrors());
    }

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
            new TopicPartition("topic.empty", 0), 100L,
            new TopicPartition("topic.empty", 1), 101L);

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty"))
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

        assertEquals(2L, actual.size());

        assertNotNull(resultPartition0);
        assertEquals(100L, resultPartition0.getSpec().getOffset());
        assertEquals(0, resultPartition0.getSpec().getPartition());
        assertEquals("topic.empty", resultPartition0.getSpec().getTopic());

        DeleteRecordsResponse resultPartition1 = actual
            .stream()
            .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 1)
            .findFirst()
            .orElse(null);

        assertNotNull(resultPartition1);
        assertEquals(101L, resultPartition1.getSpec().getOffset());
        assertEquals(1, resultPartition1.getSpec().getPartition());
        assertEquals("topic.empty", resultPartition1.getSpec().getTopic());

        verify(topicService, never()).deleteRecords(any(), anyMap());
    }

    @Test
    void deleteRecordsNotOwner() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty"))
            .thenReturn(false);

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> topicController.deleteRecords("test", "topic.empty", false));

        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Namespace not owner of this topic \"topic.empty\"."),
            actual.getValidationErrors());
    }

    @Test
    void deleteRecordsNotExistingTopic() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty"))
            .thenReturn(true);
        when(topicService.findByName(ns, "topic.empty"))
            .thenReturn(Optional.empty());

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> topicController.deleteRecords("test", "topic.empty", false));

        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Topic \"topic.empty\" does not exist."),
            actual.getValidationErrors());
    }

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
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(topicService.findCollidingTopics(ns, topic)).thenReturn(List.of("test_topic"));

        ResourceValidationException actual =
            assertThrows(ResourceValidationException.class, () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(
            List.of("Topic test.topic collides with existing topics: test_topic."),
            actual.getValidationErrors());
    }
}
