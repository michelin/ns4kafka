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
package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controller.topic.TopicController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.DeleteRecordsResponse;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.ResourceQuotaService;
import com.michelin.ns4kafka.service.TopicService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
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
    void shouldListTopicsWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.findByWildcardName(ns, "*")).thenReturn(List.of());

        assertEquals(List.of(), topicController.list("test", "*"));
    }

    @Test
    void shouldListTopicsWithWildcardParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder().name("topic1").build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder().name("topic2").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.findByWildcardName(ns, "*")).thenReturn(List.of(topic1, topic2));

        assertEquals(List.of(topic1, topic2), topicController.list("test", "*"));
    }

    @Test
    void shouldListTopicWithNoWildcardParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder().name("topic1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.findByWildcardName(ns, "topic1")).thenReturn(List.of(topic1));

        assertEquals(List.of(topic1), topicController.list("test", "topic1"));
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldGetTopicWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "topic.notfound")).thenReturn(Optional.empty());

        Optional<Topic> actual = topicController.get("test", "topic.notfound");

        assertTrue(actual.isEmpty());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldGetTopic() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "topic.found"))
                .thenReturn(Optional.of(Topic.builder()
                        .metadata(Metadata.builder().name("topic.found").build())
                        .build()));

        Optional<Topic> actual = topicController.get("test", "topic.found");

        assertTrue(actual.isPresent());
        assertEquals("topic.found", actual.get().getMetadata().getName());
    }

    @Test
    void shouldBulkDeleteTopics() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        List<Topic> toDelete = List.of(
                Topic.builder()
                        .metadata(Metadata.builder().name("prefix1.topic1").build())
                        .build(),
                Topic.builder()
                        .metadata(Metadata.builder().name("prefix1.topic2").build())
                        .build());
        when(topicService.findByWildcardName(ns, "prefix1.*")).thenReturn(toDelete);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(topicService).deleteTopics(toDelete);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        var actual = topicController.bulkDelete("test", "prefix1.*", false);
        assertEquals(HttpStatus.OK, actual.getStatus());
    }

    @Test
    void shouldNotBulkDeleteTopicsWhenNotFound() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(topicService.findByWildcardName(ns, "topic*")).thenReturn(List.of());

        var actual = topicController.bulkDelete("test", "topic*", false);

        assertEquals(HttpStatus.NOT_FOUND, actual.getStatus());
        verify(topicService, never()).delete(any());
    }

    @Test
    void shouldNotBulkDeleteTopicsInDryRunMode() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        List<Topic> toDelete = List.of(Topic.builder()
                .metadata(Metadata.builder().name("prefix.topic").build())
                .build());

        when(topicService.findByWildcardName(ns, "prefix.topic")).thenReturn(toDelete);

        var actual = topicController.bulkDelete("test", "prefix.topic", true);
        assertEquals(HttpStatus.OK, actual.getStatus());
        verify(topicService, never()).delete(any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteTopic() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        Optional<Topic> toDelete = Optional.of(Topic.builder()
                .metadata(Metadata.builder().name("topic.delete").build())
                .build());
        when(topicService.findByName(ns, "topic.delete")).thenReturn(toDelete);
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.delete")).thenReturn(true);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(topicService).delete(toDelete.get());
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> actual = topicController.delete("test", "topic.delete", false);

        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteTopicInDryRunMode() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        Optional<Topic> toDelete = Optional.of(Topic.builder()
                .metadata(Metadata.builder().name("topic.delete").build())
                .build());

        when(topicService.findByName(ns, "topic.delete")).thenReturn(toDelete);
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.delete")).thenReturn(true);

        topicController.delete("test", "topic.delete", true);

        verify(topicService, never()).delete(any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteTopicWhenUnauthorized() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.delete")).thenReturn(false);

        assertThrows(ResourceValidationException.class, () -> topicController.delete("test", "topic.delete", false));
    }

    @Test
    void shouldCreateTopic() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(resourceQuotaService.validateTopicQuota(ns, Optional.empty(), topic))
                .thenReturn(List.of());
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
    void shouldCreateTopicWithNoConstraint() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.builder().build())
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(resourceQuotaService.validateTopicQuota(ns, Optional.empty(), topic))
                .thenReturn(List.of());
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
    void shouldUpdateTopic() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
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
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
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
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic existing = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .tags(Arrays.asList("TAG1", "TAG3"))
                        .configs(Map.of(
                                "cleanup.policy", "compact", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(Arrays.asList("TAG1", "TAG2"))
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
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
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic existing = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(Arrays.asList("TAG1", "TAG2"))
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy", "compact", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .tags(new ArrayList<>(List.of("TAG1")))
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
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
        assertEquals("TAG1", actual.getSpec().getTags().getFirst());
    }

    @Test
    void shouldNotUpdateTopicWhenValidationErrors() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
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
                        .replicationFactor(3)
                        .partitions(6)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.of(existing));
        when(topicService.validateTopicUpdate(ns, existing, topic))
                .thenReturn(List.of("Invalid value 6 for configuration partitions: Value is immutable (3)."));

        ResourceValidationException actual =
                assertThrows(ResourceValidationException.class, () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(
                List.of("Invalid value 6 for configuration partitions: Value is immutable (3)."),
                actual.getValidationErrors());
    }

    @Test
    void shouldNotUpdateTopicWhenUnchanged() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
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
                        .configs(Map.of(
                                "cleanup.policy", "compact", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of(
                                "cleanup.policy", "compact", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.of(existing));

        var response = topicController.apply("test", topic, false);
        Topic actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(ArgumentMatchers.any());
        assertEquals(existing, actual);
    }

    @Test
    void shouldCreateTopicInDryRunMode() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(resourceQuotaService.validateTopicQuota(ns, Optional.empty(), topic))
                .thenReturn(List.of());

        var response = topicController.apply("test", topic, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(topic);
    }

    @Test
    void shouldNotCreateTopicWhenValidationErrors() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(1)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());

        ResourceValidationException actual =
                assertThrows(ResourceValidationException.class, () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of(".*replication\\.factor.*"), actual.getValidationErrors());
    }

    @Test
    void shouldNotFailWhenCreatingTopicWithNoValidator()
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder().topicValidator(null).build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(1)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());

        var response = topicController.apply("test", topic, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(topic);
    }

    @Test
    void shouldNotFailWhenCreatingTopicWithNoValidationConstraint()
            throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.builder().build())
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(1)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());

        var response = topicController.apply("test", topic, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(topicService, never()).create(topic);
    }

    @Test
    void shouldNotCreateTopicWhenQuotaValidationErrors() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(resourceQuotaService.validateTopicQuota(ns, Optional.empty(), topic))
                .thenReturn(List.of("Quota error"));

        ResourceValidationException actual =
                assertThrows(ResourceValidationException.class, () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Quota error"), actual.getValidationErrors());
    }

    @Test
    void shouldImportTopics() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder().name("test.topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder().name("test.topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.listUnsynchronizedTopicsByWildcardName(ns, "*")).thenReturn(List.of(topic1, topic2));
        doNothing().when(topicService).importTopics(any(), any());

        List<Topic> actual = topicController.importResources("test", "*", false);

        assertTrue(actual.stream()
                .anyMatch(t -> t.getMetadata().getName().equals("test.topic1")
                        && t.getStatus().getMessage().equals("Imported from cluster")
                        && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)));

        assertTrue(actual.stream()
                .anyMatch(t -> t.getMetadata().getName().equals("test.topic2")
                        && t.getStatus().getMessage().equals("Imported from cluster")
                        && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)));

        verify(topicService)
                .importTopics(
                        eq(ns),
                        argThat(topics -> topics.stream()
                                        .anyMatch(t -> t.getMetadata().getName().equals("test.topic1"))
                                || topics.stream()
                                        .anyMatch(t -> t.getMetadata().getName().equals("test.topic2"))));
    }

    @Test
    void shouldImportTopicInDryRunMode() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder().name("test.topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder().name("test.topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.listUnsynchronizedTopicsByWildcardName(ns, "*")).thenReturn(List.of(topic1, topic2));

        List<Topic> actual = topicController.importResources("test", "*", true);

        assertTrue(actual.stream().anyMatch(t -> t.getMetadata().getName().equals("test.topic1")));

        assertTrue(actual.stream().anyMatch(t -> t.getMetadata().getName().equals("test.topic2")));
    }

    @Test
    void shouldImportTopicWithNameParameter() throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(Metadata.builder().name("test.topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Metadata.builder().name("test.topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.listUnsynchronizedTopicsByWildcardName(ns, "test.topic1"))
                .thenReturn(List.of(topic1));
        doNothing().when(topicService).importTopics(any(), any());

        List<Topic> actual = topicController.importResources("test", "test.topic1", false);

        assertTrue(actual.stream()
                .anyMatch(t -> t.getMetadata().getName().equals("test.topic1")
                        && t.getStatus().getMessage().equals("Imported from cluster")
                        && t.getStatus().getPhase().equals(Topic.TopicPhase.Success)));

        assertFalse(actual.stream().anyMatch(t -> t.getMetadata().getName().equals("test.topic2")));

        verify(topicService).importTopics(eq(ns), argThat(topics -> topics.stream()
                .anyMatch(t -> t.getMetadata().getName().equals("test.topic1"))));
    }

    @Test
    void shouldDeleteRecords() throws ExecutionException, InterruptedException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        Topic toEmpty = Topic.builder()
                .metadata(Metadata.builder().name("topic.empty").build())
                .build();

        Map<TopicPartition, Long> partitionsToDelete = Map.of(
                new TopicPartition("topic.empty", 0), 100L,
                new TopicPartition("topic.empty", 1), 101L);

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty")).thenReturn(true);
        when(topicService.validateDeleteRecordsTopic(toEmpty)).thenReturn(List.of());
        when(topicService.findByName(ns, "topic.empty")).thenReturn(Optional.of(toEmpty));
        when(topicService.prepareRecordsToDelete(toEmpty)).thenReturn(partitionsToDelete);
        when(topicService.deleteRecords(eq(toEmpty), anyMap())).thenReturn(partitionsToDelete);

        List<DeleteRecordsResponse> actual = topicController.deleteRecords("test", "topic.empty", false);

        DeleteRecordsResponse resultPartition0 = actual.stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 0)
                .findFirst()
                .orElse(null);

        assertEquals(2L, actual.size());

        assertNotNull(resultPartition0);
        assertEquals(100L, resultPartition0.getSpec().getOffset());
        assertEquals(0, resultPartition0.getSpec().getPartition());
        assertEquals("topic.empty", resultPartition0.getSpec().getTopic());

        DeleteRecordsResponse resultPartition1 = actual.stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 1)
                .findFirst()
                .orElse(null);

        assertNotNull(resultPartition1);
        assertEquals(101L, resultPartition1.getSpec().getOffset());
        assertEquals(1, resultPartition1.getSpec().getPartition());
        assertEquals("topic.empty", resultPartition1.getSpec().getTopic());
    }

    @Test
    void shouldDeleteRecordsInCompactedTopic() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        Topic toEmpty = Topic.builder()
                .metadata(Metadata.builder().name("topic.empty").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty")).thenReturn(true);
        when(topicService.validateDeleteRecordsTopic(toEmpty))
                .thenReturn(
                        List.of("Cannot delete records on a compacted topic. Please delete and recreate the topic."));
        when(topicService.findByName(ns, "topic.empty")).thenReturn(Optional.of(toEmpty));

        ResourceValidationException actual = assertThrows(
                ResourceValidationException.class, () -> topicController.deleteRecords("test", "topic.empty", false));

        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(
                List.of("Cannot delete records on a compacted topic. Please delete and recreate the topic."),
                actual.getValidationErrors());
    }

    @Test
    void shouldDeleteRecordsInDryRunMode() throws InterruptedException, ExecutionException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        Topic toEmpty = Topic.builder()
                .metadata(Metadata.builder().name("topic.empty").build())
                .build();

        Map<TopicPartition, Long> partitionsToDelete = Map.of(
                new TopicPartition("topic.empty", 0), 100L,
                new TopicPartition("topic.empty", 1), 101L);

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty")).thenReturn(true);
        when(topicService.validateDeleteRecordsTopic(toEmpty)).thenReturn(List.of());
        when(topicService.findByName(ns, "topic.empty")).thenReturn(Optional.of(toEmpty));
        when(topicService.prepareRecordsToDelete(toEmpty)).thenReturn(partitionsToDelete);

        List<DeleteRecordsResponse> actual = topicController.deleteRecords("test", "topic.empty", true);

        DeleteRecordsResponse resultPartition0 = actual.stream()
                .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 0)
                .findFirst()
                .orElse(null);

        assertEquals(2L, actual.size());

        assertNotNull(resultPartition0);
        assertEquals(100L, resultPartition0.getSpec().getOffset());
        assertEquals(0, resultPartition0.getSpec().getPartition());
        assertEquals("topic.empty", resultPartition0.getSpec().getTopic());

        DeleteRecordsResponse resultPartition1 = actual.stream()
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
    void shouldNotDeleteRecordsWhenNotOwner() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty")).thenReturn(false);

        ResourceValidationException actual = assertThrows(
                ResourceValidationException.class, () -> topicController.deleteRecords("test", "topic.empty", false));

        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(
                List.of("Invalid value \"topic.empty\" for field \"name\": namespace is not owner of the resource."),
                actual.getValidationErrors());
    }

    @Test
    void shouldNotDeleteRecordsWhenTopicNotExist() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "topic.empty")).thenReturn(true);
        when(topicService.findByName(ns, "topic.empty")).thenReturn(Optional.empty());

        ResourceValidationException actual = assertThrows(
                ResourceValidationException.class, () -> topicController.deleteRecords("test", "topic.empty", false));

        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(
                List.of("Invalid value \"topic.empty\" for field \"name\": resource not found."),
                actual.getValidationErrors());
    }

    @Test
    void shouldNotCreateTopicWhenNameCollidesOnSpecialChar()
            throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .spec(NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Topic topic = Topic.builder()
                .metadata(Metadata.builder().name("test.topic").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "2", "retention.ms", "60000"))
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic(any(), any())).thenReturn(true);
        when(topicService.findByName(ns, "test.topic")).thenReturn(Optional.empty());
        when(topicService.findCollidingTopics(ns, topic)).thenReturn(List.of("test_topic"));

        ResourceValidationException actual =
                assertThrows(ResourceValidationException.class, () -> topicController.apply("test", topic, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(
                List.of("Invalid value \"test.topic\" for field \"name\": collision with existing topic test_topic."),
                actual.getValidationErrors());
    }
}
