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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTableflowValueSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaResponse;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class TableflowTopicServiceTest {
    @Mock
    TableflowTopicRepository tableflowTopicRepository;

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Mock
    List<ManagedClusterProperties> managedClusterProperties;

    @InjectMocks
    TableflowTopicService tableflowTopicService;

    private static Topic topic() {
        return Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("cloud")
                        .name("orders")
                        .build())
                .build();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRequireOnlyTheLatestValueSchema(boolean hasValue) {
        SchemaResponse schema = SchemaResponse.builder().schema("\"string\"").build();
        when(schemaRegistryClient.getSubject("cloud", "orders-value", "latest"))
                .thenReturn(hasValue ? Mono.just(schema) : Mono.empty());

        List<String> errors = tableflowTopicService.validateSchemas(topic()).block();

        assertEquals(hasValue ? List.of() : List.of(invalidTableflowValueSchema("orders-value")), errors);
        verify(schemaRegistryClient).getSubject("cloud", "orders-value", "latest");
        verify(schemaRegistryClient, never()).getSubject("cloud", "orders-key", "latest");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    void shouldRejectEmptyValueSchemaDefinitions(String definition) {
        when(schemaRegistryClient.getSubject("cloud", "orders-value", "latest"))
                .thenReturn(
                        Mono.just(SchemaResponse.builder().schema(definition).build()));

        assertEquals(
                List.of(invalidTableflowValueSchema("orders-value")),
                tableflowTopicService.validateSchemas(topic()).block());
    }

    @Test
    void shouldValidateTheValueSchemaWithoutBlocking() {
        Sinks.One<SchemaResponse> schema = Sinks.one();
        when(schemaRegistryClient.getSubject("cloud", "orders-value", "latest")).thenReturn(schema.asMono());

        Mono<List<String>> validation = tableflowTopicService.validateSchemas(topic());

        StepVerifier.create(validation)
                .then(() -> schema.tryEmitValue(
                        SchemaResponse.builder().schema("\"string\"").build()))
                .expectNext(List.of())
                .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 429, 500})
    void shouldPropagateSchemaRegistryFailuresToTheExceptionHandler(int status) {
        HttpClientResponseException exception = new HttpClientResponseException(
                "Schema Registry unavailable", HttpResponse.status(HttpStatus.valueOf(status)));
        when(schemaRegistryClient.getSubject("cloud", "orders-value", "latest")).thenReturn(Mono.error(exception));

        StepVerifier.create(tableflowTopicService.validateSchemas(topic()))
                .expectErrorMatches(error -> error == exception)
                .verify();
        verifyNoInteractions(tableflowTopicRepository);
    }

    @Test
    void shouldPropagateSchemaRegistryTimeout() {
        when(schemaRegistryClient.getSubject("cloud", "orders-value", "latest"))
                .thenReturn(Mono.error(ReadTimeoutException.TIMEOUT_EXCEPTION));

        StepVerifier.create(tableflowTopicService.validateSchemas(topic()))
                .expectError(ReadTimeoutException.class)
                .verify();
    }

    @Test
    void shouldRejectMissingSchemaRegistryConfiguration() {
        Topic topic = topic();
        when(schemaRegistryClient.getSubject("cloud", "orders-value", "latest"))
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
        verifyNoInteractions(managedClusterProperties, schemaRegistryClient);
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

    private static Namespace namespace() {
        return Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("sales")
                        .cluster("cloud")
                        .build())
                .build();
    }

    private static ManagedClusterProperties cloudCluster() {
        ManagedClusterProperties cluster =
                new ManagedClusterProperties("cloud", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        ManagedClusterProperties.ConfluentCloudProperties config =
                new ManagedClusterProperties.ConfluentCloudProperties();
        config.setClusterId("lkc-test");
        config.setEnvironmentId("env-test");
        config.setBasicAuthUsername("shared-key");
        config.setBasicAuthPassword("shared-secret");
        cluster.setConfluentCloud(config);
        return cluster;
    }

    @Test
    void shouldValidateTheNamespacesClusterWithoutAccessingSchemasOrStorage() {
        ManagedClusterProperties otherCluster =
                new ManagedClusterProperties("other", ManagedClusterProperties.KafkaProvider.SELF_MANAGED);
        when(managedClusterProperties.stream()).thenReturn(Stream.of(otherCluster, cloudCluster()));

        tableflowTopicService.validateConfiguration(namespace(), "orders");

        verifyNoInteractions(schemaRegistryClient, tableflowTopicRepository);
    }

    @ParameterizedTest
    @ValueSource(strings = {"unknown", "self-managed"})
    void shouldRejectUnsupportedClusters(String type) {
        ManagedClusterProperties cluster = cloudCluster();
        if (type.equals("unknown")) {
            cluster.setName("other");
        } else {
            cluster.setProvider(ManagedClusterProperties.KafkaProvider.SELF_MANAGED);
        }
        when(managedClusterProperties.stream()).thenReturn(Stream.of(cluster));
        Namespace ns = namespace();

        ResourceValidationException error = assertThrows(
                ResourceValidationException.class, () -> tableflowTopicService.validateConfiguration(ns, "orders"));

        assertTrue(error.getValidationErrors().contains("Tableflow is supported only for Confluent Cloud clusters."));
        verifyNoInteractions(schemaRegistryClient, tableflowTopicRepository);
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "cluster", "environment", "key", "secret"})
    void shouldRejectIncompleteClusterConfiguration(String field) {
        ManagedClusterProperties cluster = cloudCluster();
        ManagedClusterProperties.ConfluentCloudProperties config = cluster.getConfluentCloud();
        switch (field) {
            case "missing" -> cluster.setConfluentCloud(null);
            case "cluster" -> config.setClusterId(null);
            case "environment" -> config.setEnvironmentId(null);
            case "key" -> config.setBasicAuthUsername("");
            case "secret" -> config.setBasicAuthPassword(null);
            default -> throw new IllegalArgumentException(field);
        }
        when(managedClusterProperties.stream()).thenReturn(Stream.of(cluster));
        Namespace ns = namespace();

        assertThrows(
                ResourceValidationException.class, () -> tableflowTopicService.validateConfiguration(ns, "orders"));

        verifyNoInteractions(schemaRegistryClient, tableflowTopicRepository);
    }
}
