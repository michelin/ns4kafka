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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.kafka.DelayStartupListener;
import com.michelin.ns4kafka.repository.kafka.KafkaNamespaceRepository;
import com.michelin.ns4kafka.security.auth.AuthenticationService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.TableflowTopicService;
import com.michelin.ns4kafka.service.TopicService;
import com.michelin.ns4kafka.util.enumation.Kind;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TableflowTopicValidator;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.runtime.server.EmbeddedServer;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TableflowTopicHttpTest {
    private NamespaceService namespaceService;
    private TopicService topicService;
    private TableflowTopicService tableflowTopicService;
    private EmbeddedServer server;
    private HttpClient client;

    @BeforeEach
    void setUp() {
        server = ApplicationContext.builder()
                .enableDefaultPropertySources(false)
                .properties(Map.of(
                        "tableflow.http-test",
                        true,
                        "micronaut.security.enabled",
                        true,
                        "micronaut.security.intercept-url-map",
                        List.of(Map.of("pattern", "/api/**", "access", List.of("isAnonymous()"))),
                        "micronaut.server.port",
                        -1,
                        "ns4kafka.log.console.enabled",
                        false))
                .run(EmbeddedServer.class);
        namespaceService = server.getApplicationContext().getBean(NamespaceService.class);
        topicService = server.getApplicationContext().getBean(TopicService.class);
        tableflowTopicService = server.getApplicationContext().getBean(TableflowTopicService.class);
        client = HttpClient.create(server.getURL());
    }

    @Factory
    @Requires(property = "tableflow.http-test", value = "true")
    static class TestServices {
        @Singleton
        @Replaces(AuthenticationService.class)
        AuthenticationService authenticationService() {
            return mock(AuthenticationService.class);
        }

        @Singleton
        @Replaces(NamespaceService.class)
        NamespaceService namespaceService() {
            return mock(NamespaceService.class);
        }

        @Singleton
        @Replaces(TopicService.class)
        TopicService topicService() {
            return mock(TopicService.class);
        }

        @Singleton
        @Replaces(TableflowTopicService.class)
        TableflowTopicService tableflowTopicService() {
            return mock(TableflowTopicService.class);
        }

        @Singleton
        @Replaces(KafkaNamespaceRepository.class)
        NamespaceRepository namespaceRepository() {
            return mock(NamespaceRepository.class);
        }

        @Singleton
        @Replaces(DelayStartupListener.class)
        DelayStartupListener delayStartupListener() {
            return mock(DelayStartupListener.class);
        }
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.getApplicationContext().close();
        }
    }

    @ParameterizedTest
    @CsvSource({"false,ICEBERG", "true,ICEBERG", "false,DELTA", "true,DELTA"})
    void shouldCreateOrPreviewTheSeparateResourceOverHttp(boolean dryrun, String format) {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("sales")
                        .cluster("cloud")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .tableflowTopicValidator(TableflowTopicValidator.builder()
                                .validationConstraints(Map.of(
                                        "table.format", ResourceValidator.ValidList.in(format),
                                        "storage.kind", ResourceValidator.ValidString.in("Managed")))
                                .build())
                        .build())
                .build();
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .name("sales.orders")
                        .cluster("cloud")
                        .build())
                .build();
        when(namespaceService.findByName("sales")).thenReturn(Optional.of(namespace));
        when(topicService.isNamespaceOwnerOfTopic("sales", "sales.orders")).thenReturn(true);
        when(topicService.findByName(namespace, "sales.orders")).thenReturn(Optional.of(topic));
        when(tableflowTopicService.validateSchemas(topic)).thenReturn(List.of());
        when(tableflowTopicService.create(any())).thenAnswer(invocation -> {
            TableflowTopic submitted = invocation.getArgument(0);
            submitted.setStatus(TableflowTopic.TableflowTopicStatus.builder()
                    .id("tt-test")
                    .state(JsonNode.from(Map.of("phase", "PROVISIONING")))
                    .build());
            return submitted;
        });

        var response = client.toBlocking()
                .exchange(
                        HttpRequest.POST("/api/namespaces/sales/tableflow-topics?dryrun=" + dryrun, """
                {"apiVersion":"v1","kind":"TableflowTopic","metadata":{"name":"sales.orders"},
                 "spec":{"table_formats":["%s"],"storage":{"kind":"Managed"}}}
                """.formatted(
                                                format))
                                .contentType(MediaType.APPLICATION_JSON_TYPE),
                        TableflowTopic.class);

        assertEquals(dryrun ? HttpStatus.OK : HttpStatus.ACCEPTED, response.getStatus());
        assertEquals("created", response.getHeaders().get("X-Ns4kafka-Result"));
        assertEquals(Kind.TABLEFLOW_TOPIC, response.body().getKind());
        assertEquals("sales.orders", response.body().getMetadata().getName());
        assertEquals("sales", response.body().getMetadata().getNamespace());
        assertEquals("cloud", response.body().getMetadata().getCluster());
        assertEquals("Managed", response.body().getSpec().getStorage().getKind());
        assertEquals(List.of(format), response.body().getSpec().getTableFormats());
        if (dryrun) {
            assertNull(response.body().getStatus());
            verify(tableflowTopicService, never()).create(any());
        } else {
            assertEquals("tt-test", response.body().getStatus().getId());
        }
        verify(tableflowTopicService).validateSchemas(topic);
        verify(topicService, never()).create(any());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{\"metadata\":{\"name\":\"sales.orders\"},\"spec\":{\"table_formats\":[\"DELTA\"],\"storage\":{\"kind\":\"Managed\"}}}",
                "{\"metadata\":{\"name\":\"sales.orders\"},\"spec\":{\"table_formats\":[\"ICEBERG\"],\"storage\":{\"kind\":\"ByobAws\"}}}",
                "{\"metadata\":{},\"spec\":{\"table_formats\":[\"ICEBERG\"],\"storage\":{\"kind\":\"Managed\"}}}",
                "{\"metadata\":{\"name\":\"sales.orders\"}}",
                "{\"metadata\":{\"name\":\"sales.orders\"},\"spec\":{}}",
                "{\"metadata\":{\"name\":\"sales.orders\"},\"spec\":{\"table_formats\":[],\"storage\":{\"kind\":\"Managed\"}}}",
                "{\"metadata\":{\"name\":\"sales.orders\"},\"spec\":{\"table_formats\":[null],\"storage\":{\"kind\":\"Managed\"}}}",
                "{\"metadata\":{\"name\":\"sales.orders\"},\"spec\":{\"table_formats\":[\"\"],\"storage\":{\"kind\":\"Managed\"}}}",
                "{\"metadata\":{\"name\":\"sales.orders\"},\"spec\":{\"table_formats\":[\"ICEBERG\"]}}",
                "{\"metadata\":{\"name\":\"sales.orders\"},\"spec\":{\"table_formats\":[\"ICEBERG\"],\"storage\":{}}}"
            })
    void shouldRejectUnsupportedOptionsAndInvalidMetadataOverHttp(String body) {
        when(namespaceService.findByName("sales"))
                .thenReturn(Optional.of(Namespace.builder()
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("user")
                                .tableflowTopicValidator(TableflowTopicValidator.builder()
                                        .validationConstraints(Map.of(
                                                "table.format", ResourceValidator.ValidList.in("ICEBERG"),
                                                "storage.kind", ResourceValidator.ValidString.in("Managed")))
                                        .build())
                                .build())
                        .build()));
        when(topicService.isNamespaceOwnerOfTopic("sales", "sales.orders")).thenReturn(true);

        var error = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking()
                        .exchange(HttpRequest.POST("/api/namespaces/sales/tableflow-topics", body)
                                .contentType(MediaType.APPLICATION_JSON_TYPE)));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, error.getStatus());
        verify(tableflowTopicService, never()).create(any());
        verify(tableflowTopicService, never()).validateSchemas(any());
    }
}
