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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
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
import io.micronaut.json.JsonMapper;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
                                        "table.format", ResourceValidator.ValidList.in("ICEBERG"),
                                        "storage.kind", ResourceValidator.ValidString.in("Managed")))
                                .build())
                        .build())
                .build();
    }

    private static TableflowTopic tableflowRequest(TableflowTopic.TableflowTopicSpec spec) {
        return TableflowTopic.builder()
                .metadata(Resource.Metadata.builder().name("orders").build())
                .spec(spec)
                .build();
    }

    private static TableflowTopic namedRequest(String name) {
        var request = icebergRequest();
        request.getMetadata().setName(name);
        return request;
    }

    private static TableflowTopic icebergRequest() {
        return tableflowRequest(TableflowTopic.TableflowTopicSpec.builder()
                .tableFormats(List.of("ICEBERG"))
                .storage(TableflowTopic.Storage.builder().kind("Managed").build())
                .build());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldRejectInvalidConfigurationBeforeSchemaLookupOrCreation(boolean dryrun) {
        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace()));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        TableflowTopic request = icebergRequest();
        request.getSpec().setTableFormats(List.of("DELTA"));

        var error = assertThrows(
                ResourceValidationException.class, () -> tableflowTopicController.apply("test", request, dryrun));

        assertTrue(error.getValidationErrors().stream().anyMatch(message -> message.contains("table.format")));
        verify(topicService, never()).findByName(any(Namespace.class), any());
        verify(tableflowTopicService, never()).validateSchemas(any());
        verify(tableflowTopicService, never()).create(any());
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldCreateTableflowForOwnedTopicAndAuditAcceptance() throws Exception {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic createdTableflowTopic = icebergRequest();
        createdTableflowTopic.setStatus(TableflowTopic.TableflowTopicStatus.builder()
                .id("tt-test")
                .state(JsonNode.from(Map.of("phase", "PROVISIONING")))
                .build());
        createdTableflowTopic.getMetadata().setStatus(Resource.Metadata.Status.ofPending());
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(List.of());
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

        var request = JsonMapper.createDefault()
                .readValue(
                        "{\"metadata\":{\"name\":\"orders\"},\"spec\":{\"table_formats\":[\"ICEBERG\"],\"storage\":{\"kind\":\"Managed\"}}}",
                        TableflowTopic.class);
        var response = tableflowTopicController.apply("test", request, false);

        assertEquals(HttpStatus.ACCEPTED, response.getStatus());
        assertEquals("tt-test", response.body().getStatus().getId());
        assertEquals(
                "PROVISIONING",
                response.body().getStatus().getState().get("phase").getStringValue());
        assertEquals(
                Resource.Metadata.Phase.PENDING,
                response.body().getMetadata().getStatus().getPhase());
        assertEquals(Kind.TABLEFLOW_TOPIC, response.body().getKind());
        assertEquals("cloud", response.body().getMetadata().getCluster());
        assertEquals("test", response.body().getMetadata().getNamespace());
        assertEquals("Managed", response.body().getSpec().getStorage().getKind());
        var calls = inOrder(tableflowTopicService);
        calls.verify(tableflowTopicService).validateSchemas(topic);
        calls.verify(tableflowTopicService).create(request);
        var audit = ArgumentCaptor.forClass(AuditLog.class);
        verify(applicationEventPublisher).publishEvent(audit.capture());
        assertEquals(Kind.TABLEFLOW_TOPIC, audit.getValue().getKind());
        assertEquals(ApplyStatus.CREATED, audit.getValue().getOperation());
        verify(topicService, never()).create(any());
    }

    @Test
    void shouldRejectTableflowForTopicOutsideNamespace() {
        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace()));
        when(topicService.isNamespaceOwnerOfTopic("test", "other.orders")).thenReturn(false);

        assertThrows(
                ResourceValidationException.class,
                () -> tableflowTopicController.apply("test", namedRequest("other.orders"), false));

        verify(tableflowTopicService, never()).create(any());
        verify(topicService, never()).findByName(any(Namespace.class), any());
    }

    @Test
    void shouldRejectTableflowForMissingTopic() {
        Namespace ns = namespace();
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.empty());

        assertThrows(
                ResourceValidationException.class,
                () -> tableflowTopicController.apply("test", icebergRequest(), false));

        verify(tableflowTopicService, never()).create(any());
    }

    @Test
    void shouldRejectTableflowForUndeployedOrDeletingTopic() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(0).build())
                .build();
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));

        assertThrows(
                ResourceValidationException.class,
                () -> tableflowTopicController.apply("test", icebergRequest(), false));
        topic.getMetadata().setGeneration(1);
        topic.getMetadata()
                .setStatus(Resource.Metadata.Status.builder()
                        .phase(Resource.Metadata.Phase.DELETING)
                        .build());
        assertThrows(
                ResourceValidationException.class,
                () -> tableflowTopicController.apply("test", icebergRequest(), false));

        verify(tableflowTopicService, never()).create(any());
    }

    @Test
    void shouldNotSubmitTableflowOrAuditDuringDryRun() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(List.of());

        TableflowTopic request = icebergRequest();
        request.getMetadata().setCluster("caller-cluster");
        request.getMetadata().setNamespace("caller-namespace");
        request.setStatus(TableflowTopic.TableflowTopicStatus.builder()
                .id("caller-status")
                .build());

        var response = tableflowTopicController.apply("test", request, true);

        assertEquals(HttpStatus.OK, response.getStatus());
        assertEquals(ApplyStatus.CREATED.toString(), response.getHeaders().get("X-Ns4kafka-Result"));
        assertEquals("cloud", response.body().getMetadata().getCluster());
        assertEquals("test", response.body().getMetadata().getNamespace());
        assertEquals(List.of("ICEBERG"), response.body().getSpec().getTableFormats());
        assertEquals("Managed", response.body().getSpec().getStorage().getKind());
        assertNull(response.body().getStatus());

        verify(tableflowTopicService, never()).create(any());
        verify(tableflowTopicService).validateSchemas(topic);
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @Test
    void shouldNotAuditCreationWhenConfluentSubmissionFails() {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.create(any()))
                .thenThrow(new HttpStatusException(HttpStatus.BAD_GATEWAY, "Confluent unavailable"));

        assertThrows(HttpStatusException.class, () -> tableflowTopicController.apply("test", icebergRequest(), false));

        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldRejectMissingSchemasBeforeCreationIncludingDryRun(boolean dryrun) {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder().name("orders").build())
                .build();
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        List<String> errors = List.of("Tableflow requires a registered schema for subject [orders-value].");
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(errors);

        var error = assertThrows(
                ResourceValidationException.class,
                () -> tableflowTopicController.apply("test", icebergRequest(), dryrun));

        assertEquals(errors, error.getValidationErrors());
        verify(tableflowTopicService, never()).create(any());
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldStopWhenSchemaRegistryValidationFails(boolean dryrun) {
        Namespace ns = namespace();
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder().name("orders").build())
                .build();
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic))
                .thenThrow(new HttpStatusException(HttpStatus.BAD_GATEWAY, "Schema Registry unavailable"));

        assertThrows(HttpStatusException.class, () -> tableflowTopicController.apply("test", icebergRequest(), dryrun));

        verify(tableflowTopicService, never()).create(any());
        verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldPreserveNamespaceApprovedSettingsAndWarnings(boolean dryrun) {
        Namespace ns = namespace();
        ResourceValidator.Validator softRule = (name, value) -> {
            throw new FieldValidationException("Prefer Iceberg", true);
        };
        ns.getSpec()
                .setTableflowTopicValidator(TableflowTopicValidator.builder()
                        .validationConstraints(Map.of("table.format", softRule))
                        .build());
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        TableflowTopic request = icebergRequest();
        request.getSpec().setTableFormats(List.of("DELTA"));
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));
        if (!dryrun) {
            when(tableflowTopicService.create(request)).thenReturn(request);
        }

        var response = tableflowTopicController.apply("test", request, dryrun);

        assertEquals(dryrun ? HttpStatus.OK : HttpStatus.ACCEPTED, response.getStatus());
        assertEquals("Prefer Iceberg", response.getHeaders().get("X-Ns4kafka-Warnings"));
        assertEquals(List.of("DELTA"), response.body().getSpec().getTableFormats());
        if (dryrun) {
            verify(tableflowTopicService, never()).create(any());
            verify(applicationEventPublisher, never()).publishEvent(any(AuditLog.class));
        } else {
            verify(tableflowTopicService).create(request);
        }
    }

    @Test
    void shouldAllowAnAbsentValidatorLikeOtherResourceControllers() {
        Namespace ns = namespace();
        ns.getSpec().setTableflowTopicValidator(null);
        Topic topic = Topic.builder()
                .metadata(
                        Resource.Metadata.builder().name("orders").generation(1).build())
                .build();
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(topicService.isNamespaceOwnerOfTopic("test", "orders")).thenReturn(true);
        when(topicService.findByName(ns, "orders")).thenReturn(Optional.of(topic));

        assertEquals(
                HttpStatus.OK,
                tableflowTopicController.apply("test", icebergRequest(), true).getStatus());
        verify(tableflowTopicService, never()).create(any());
    }
}
