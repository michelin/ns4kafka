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
package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaResponse;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.micronaut.http.exceptions.HttpStatusException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class TableflowTopicServiceTest {
    @Mock
    TableflowTopicRepository tableflowTopicRepository;

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Mock
    ConfluentCloudClient confluentCloudClient;

    @InjectMocks
    TableflowTopicService tableflowTopicService;

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void shouldRequireLatestKeyAndValueSchemas(boolean hasKey, boolean hasValue) {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cloud")
                        .name("orders")
                        .build())
                .build();
        var schema = SchemaResponse.builder().schema("\"string\"").build();
        when(schemaRegistryClient.getSubject("cloud", "orders-key", "latest"))
                .thenReturn(hasKey ? Mono.just(schema) : Mono.empty());
        when(schemaRegistryClient.getSubject("cloud", "orders-value", "latest"))
                .thenReturn(hasValue ? Mono.just(schema) : Mono.empty());

        List<String> errors = tableflowTopicService.validateSchemas(topic);

        List<String> expected = new ArrayList<>();
        if (!hasKey) {
            expected.add("Tableflow requires a registered schema for subject [orders-key].");
        }
        if (!hasValue) {
            expected.add("Tableflow requires a registered schema for subject [orders-value].");
        }
        assertEquals(expected, errors);
        verify(schemaRegistryClient).getSubject("cloud", "orders-key", "latest");
        verify(schemaRegistryClient).getSubject("cloud", "orders-value", "latest");
    }

    @Test
    void shouldRejectEmptySchemaDefinitions() {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cloud")
                        .name("orders")
                        .build())
                .build();
        when(schemaRegistryClient.getSubject("cloud", "orders-key", "latest"))
                .thenReturn(Mono.just(SchemaResponse.builder().schema(" ").build()));
        when(schemaRegistryClient.getSubject("cloud", "orders-value", "latest"))
                .thenReturn(Mono.just(SchemaResponse.builder().build()));

        assertEquals(2, tableflowTopicService.validateSchemas(topic).size());
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 429, 500})
    void shouldReportSchemaRegistryFailuresWithoutCallingTheExecutor(int status) {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cloud")
                        .name("orders")
                        .build())
                .build();
        when(schemaRegistryClient.getSubject("cloud", "orders-key", "latest"))
                .thenReturn(Mono.error(new HttpClientResponseException(
                        "DO-NOT-EXPOSE", HttpResponse.status(HttpStatus.valueOf(status)))));

        var error = assertThrows(HttpStatusException.class, () -> tableflowTopicService.validateSchemas(topic));

        assertEquals(HttpStatus.BAD_GATEWAY, error.getStatus());
        assertTrue(error.getMessage().contains("HTTP " + status));
        assertTrue(!error.getMessage().contains("DO-NOT-EXPOSE"));
        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldReportSchemaRegistryTimeout() {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cloud")
                        .name("orders")
                        .build())
                .build();
        when(schemaRegistryClient.getSubject("cloud", "orders-key", "latest"))
                .thenReturn(Mono.error(ReadTimeoutException.TIMEOUT_EXCEPTION));

        var error = assertThrows(HttpStatusException.class, () -> tableflowTopicService.validateSchemas(topic));

        assertEquals(HttpStatus.GATEWAY_TIMEOUT, error.getStatus());
        assertTrue(error.getMessage().contains("Tableflow creation was not attempted"));
    }

    @Test
    void shouldRejectMissingSchemaRegistryConfiguration() {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cloud")
                        .name("orders")
                        .build())
                .build();
        when(schemaRegistryClient.getSubject("cloud", "orders-key", "latest"))
                .thenThrow(new ResourceValidationException(topic, "Schema Registry is not configured."));

        assertThrows(ResourceValidationException.class, () -> tableflowTopicService.validateSchemas(topic));
    }

    @Test
    void shouldStoreTableflowConfigurationWithoutCallingConfluent() {
        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cloud")
                        .name("orders")
                        .generation(0)
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(List.of("ICEBERG"))
                        .build())
                .build();
        when(tableflowTopicRepository.create(tableflowTopic)).thenReturn(tableflowTopic);

        assertEquals(tableflowTopic, tableflowTopicService.create(tableflowTopic));
        assertTrue(tableflowTopic.isPending());
        assertEquals(0, tableflowTopic.getMetadata().getGeneration());
        verify(tableflowTopicRepository).create(tableflowTopic);
        verify(confluentCloudClient).validateTableflowConfiguration("cloud", "orders");
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldFindConfigurationInTheNamespacesCluster() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("sales")
                        .cluster("cloud")
                        .build())
                .build();
        tableflowTopicService.findByName(namespace, "orders");
        verify(tableflowTopicRepository).findByName("cloud", "orders");
    }

    @Test
    void shouldNotStoreRequestsWithInvalidLocalClusterConfiguration() {
        TableflowTopic request = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name("orders")
                        .cluster("cloud")
                        .build())
                .build();
        doThrow(new ResourceValidationException(request, "Tableflow credentials are missing."))
                .when(confluentCloudClient)
                .validateTableflowConfiguration("cloud", "orders");

        assertThrows(ResourceValidationException.class, () -> tableflowTopicService.create(request));

        verify(tableflowTopicRepository, never()).create(any());
    }
}
