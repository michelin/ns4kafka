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
package com.michelin.ns4kafka.controller.tableflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.TableflowTopicService;
import com.michelin.ns4kafka.service.TopicService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.enumation.Kind;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import com.michelin.ns4kafka.validation.FieldValidationException;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TableflowTopicValidator;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class TableflowTopicControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    TopicService topicService;

    @Mock
    TableflowTopicService tableflowTopicService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    TableflowTopicController tableflowTopicController;

    @Test
    void shouldCreateTableflowTopic() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic request = icebergRequest();
        TableflowTopic createdTableflowTopic = icebergRequest();
        createdTableflowTopic.getMetadata().setStatus(Resource.Metadata.Status.ofPending());

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(Mono.just(List.of()));
        when(tableflowTopicService.create(any())).thenAnswer(invocation -> {
            TableflowTopic submitted = invocation.getArgument(0);
            createdTableflowTopic
                    .getMetadata()
                    .setCluster(submitted.getMetadata().getCluster());
            createdTableflowTopic
                    .getMetadata()
                    .setNamespace(submitted.getMetadata().getNamespace());
            createdTableflowTopic.setSpec(submitted.getSpec());
            return createdTableflowTopic;
        });
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeNextWith(response -> {
                    TableflowTopic actual = response.body();
                    assertNotNull(actual);
                    assertEquals(HttpStatus.OK, response.getStatus());
                    assertNull(actual.getStatus());
                    assertEquals(
                            Resource.Metadata.Phase.PENDING,
                            actual.getMetadata().getStatus().getPhase());
                    assertEquals(Kind.TABLEFLOW_TOPIC, actual.getKind());
                    assertEquals("cloud", actual.getMetadata().getCluster());
                    assertEquals("test", actual.getMetadata().getNamespace());
                    assertEquals("Managed", actual.getSpec().getStorage().getKind());
                })
                .verifyComplete();

        verify(tableflowTopicService).validateConfiguration(ns, "orders");
        verify(tableflowTopicService).validateSchemas(topic);
        verify(tableflowTopicService).create(request);
        verify(applicationEventPublisher).publishEvent(any(AuditLog.class));
        verify(topicService, never()).create(any());
    }

    @Test
    void shouldCreateTableflowTopicInDryRunMode() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic request = icebergRequest();
        request.getMetadata().setCluster("caller-cluster");
        request.getMetadata().setNamespace("caller-namespace");

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(Mono.just(List.of()));

        StepVerifier.create(tableflowTopicController.apply("test", request, true))
                .consumeNextWith(response -> {
                    TableflowTopic actual = response.body();
                    assertNotNull(actual);
                    assertEquals(HttpStatus.OK, response.getStatus());
                    assertEquals(
                            ApplyStatus.CREATED.toString(),
                            response.getHeaders().get("X-Ns4kafka-Result"));
                    assertEquals("cloud", actual.getMetadata().getCluster());
                    assertEquals("test", actual.getMetadata().getNamespace());
                    assertEquals(Set.of("ICEBERG"), actual.getSpec().getTableFormats());
                    assertEquals("Managed", actual.getSpec().getStorage().getKind());
                    assertNull(actual.getStatus());
                })
                .verifyComplete();

        verify(tableflowTopicService, never()).create(any());
        verify(tableflowTopicService).validateSchemas(topic);
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldCreateTableflowTopicWithValidationWarnings() {
        Namespace ns = namespace();
        ns.getSpec()
                .setTableflowTopicValidator(TableflowTopicValidator.builder()
                        .validationConstraints(Map.of("tableFormats", (name, value) -> {
                            throw new FieldValidationException("Prefer Iceberg", true);
                        }))
                        .build());
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic request = icebergRequest();
        request.getSpec().setTableFormats(Set.of("DELTA"));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(Mono.just(List.of()));
        when(tableflowTopicService.create(request)).thenReturn(request);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeNextWith(response -> {
                    assertEquals(HttpStatus.OK, response.getStatus());
                    TableflowTopic actual = response.body();
                    assertNotNull(actual);
                    assertEquals("Prefer Iceberg", response.getHeaders().get("X-Ns4kafka-Warnings"));
                    assertEquals(Set.of("DELTA"), actual.getSpec().getTableFormats());
                })
                .verifyComplete();

        verify(tableflowTopicService).create(request);
    }

    @Test
    void shouldNotFailWhenCreatingTableflowTopicWithNoValidator() {
        Namespace ns = namespace();
        ns.getSpec().setTableflowTopicValidator(null);
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(Mono.just(List.of()));

        StepVerifier.create(tableflowTopicController.apply("test", icebergRequest(), true))
                .consumeNextWith(response -> assertEquals(HttpStatus.OK, response.getStatus()))
                .verifyComplete();

        verify(tableflowTopicService, never()).create(any());
    }

    @Test
    void shouldNotCreateTableflowTopicWhenValidationErrors() {
        TableflowTopic request = icebergRequest();
        request.getSpec().setTableFormats(Set.of("DELTA"));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace()));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertTrue(((ResourceValidationException) error)
                            .getValidationErrors().stream().anyMatch(message -> message.contains("tableFormats")));
                })
                .verify();

        verify(tableflowTopicService, never()).validateSchemas(any());
        verify(tableflowTopicService, never()).create(any());
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldNotCreateTableflowTopicWhenNotOwner() {
        TableflowTopic request = icebergRequest();
        request.getMetadata().setName("other.orders");

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace()));
        when(topicService.isNamespaceOwnerOfTopic("test", "other.orders")).thenReturn(false);

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> assertEquals(ResourceValidationException.class, error.getClass()))
                .verify();

        verify(tableflowTopicService, never()).create(any());
        verify(topicService, never()).findByName(any(Namespace.class), any());
    }

    @Test
    void shouldNotCreateTableflowTopicWhenTopicIsMissing() {
        Namespace ns = namespace();
        TableflowTopic request = icebergRequest();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.empty());

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            List.of(
                                    "Invalid value \"orders\" for field \"name\": a topic with the same name must exist."),
                            ((ResourceValidationException) error).getValidationErrors());
                })
                .verify();

        verify(tableflowTopicService, never()).validateSchemas(any());
        verify(tableflowTopicService, never()).create(any());
    }

    @Test
    void shouldNotCreateTableflowTopicWhenClusterIsNotConfigured() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic request = icebergRequest();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(tableflowTopicService.validateConfiguration(ns, "orders"))
                .thenReturn(List.of("Invalid \"orders\": the cluster is not configured to use Tableflow."));
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            List.of("Invalid \"orders\": the cluster is not configured to use Tableflow."),
                            ((ResourceValidationException) error).getValidationErrors());
                })
                .verify();

        verify(tableflowTopicService, never()).validateSchemas(any());
        verify(tableflowTopicService, never()).create(any());
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldNotCreateTableflowTopicWithAllValidationErrors() {
        Namespace ns = namespace();
        TableflowTopic request = icebergRequest();
        request.getSpec().setTableFormats(Set.of("DELTA"));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(tableflowTopicService.validateConfiguration(ns, "orders"))
                .thenReturn(List.of("Invalid \"orders\": the cluster is not configured to use Tableflow."));
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.empty());

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    List<String> validationErrors = ((ResourceValidationException) error).getValidationErrors();
                    assertEquals(3, validationErrors.size());
                    assertTrue(validationErrors.stream().anyMatch(message -> message.contains("tableFormats")));
                    assertTrue(validationErrors.contains(
                            "Invalid \"orders\": the cluster is not configured to use Tableflow."));
                    assertTrue(validationErrors.contains(
                            "Invalid value \"orders\" for field \"name\": a topic with the same name must exist."));
                })
                .verify();

        verify(tableflowTopicService, never()).validateSchemas(any());
        verify(tableflowTopicService, never()).create(any());
    }

    @Test
    void shouldNotCreateTableflowTopicWhenValueSchemaIsMissing() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder().name("orders").build())
                .build();
        TableflowTopic request = icebergRequest();
        List<String> errors =
                List.of(com.michelin.ns4kafka.util.FormatErrorUtils.invalidTableflowValueSchema("orders-value"));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(Mono.just(errors));

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(errors, ((ResourceValidationException) error).getValidationErrors());
                })
                .verify();

        verify(tableflowTopicService, never()).create(any());
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldNotCreateTableflowTopicWhenSchemaRegistryFails() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder().name("orders").build())
                .build();
        TableflowTopic request = icebergRequest();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic))
                .thenReturn(Mono.error(new HttpStatusException(HttpStatus.BAD_GATEWAY, "Schema Registry unavailable")));

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> assertEquals(HttpStatusException.class, error.getClass()))
                .verify();

        verify(tableflowTopicService, never()).create(any());
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldNotSendAuditLogWhenCreationFails() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic request = icebergRequest();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(Mono.just(List.of()));
        when(tableflowTopicService.create(any()))
                .thenThrow(new HttpStatusException(HttpStatus.BAD_GATEWAY, "Storage unavailable"));

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> assertEquals(HttpStatusException.class, error.getClass()))
                .verify();

        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldNotUpdateTableflowTopicWhenUnchanged() {
        Namespace ns = namespace();
        Topic source = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic stored = icebergRequest();
        stored.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(source));
        when(tableflowTopicService.validateSchemas(source)).thenReturn(Mono.just(List.of()));
        when(tableflowTopicService.findByName(ns, "orders")).thenReturn(Optional.of(stored));

        StepVerifier.create(tableflowTopicController.apply("test", icebergRequest(), false))
                .consumeNextWith(response -> {
                    assertEquals(HttpStatus.OK, response.getStatus());
                    assertEquals("unchanged", response.getHeaders().get("X-Ns4kafka-Result"));
                    assertEquals(stored, response.body());
                })
                .verifyComplete();

        verify(tableflowTopicService, never()).create(any());
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldNotUpdateTableflowTopic() {
        Namespace ns = namespace();
        Topic source = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic stored = icebergRequest();
        stored.getSpec().setTableFormats(Set.of("DELTA"));
        TableflowTopic request = icebergRequest();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(source));
        when(tableflowTopicService.validateSchemas(source)).thenReturn(Mono.just(List.of()));
        when(tableflowTopicService.findByName(ns, "orders")).thenReturn(Optional.of(stored));

        StepVerifier.create(tableflowTopicController.apply("test", request, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertTrue(((ResourceValidationException) error)
                            .getValidationErrors()
                            .contains("Updating Tableflow configuration is not supported yet."));
                })
                .verify();

        verify(tableflowTopicService, never()).create(any());
    }

    @Test
    void shouldListTableflowTopics() {
        Namespace ns = namespace();
        TableflowTopic tableflowTopic = icebergRequest();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(tableflowTopicService.findByWildcardName(ns, "*")).thenReturn(Flux.just(tableflowTopic));

        StepVerifier.create(tableflowTopicController.list("test", "*"))
                .expectNext(tableflowTopic)
                .verifyComplete();
    }

    private static Namespace namespace() {
        return Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("test")
                        .cluster("cloud")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .tableflowTopicValidator(TableflowTopicValidator.builder()
                                .validationConstraints(Map.of(
                                        "tableFormats", ResourceValidator.ValidList.in("ICEBERG"),
                                        "storage.kind", ResourceValidator.ValidString.in("Managed")))
                                .build())
                        .build())
                .build();
    }

    private static TableflowTopic icebergRequest() {
        return TableflowTopic.builder()
                .metadata(Resource.Metadata.builder().name("orders").build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();
    }
}
