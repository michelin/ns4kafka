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
package com.michelin.ns4kafka.service.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
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
class TableflowTopicAsyncExecutorTest {
    private static final Instant INSTANT = Instant.parse("2026-01-01T00:00:00Z");
    private static final String CLUSTER = "local";
    private static final String TOPIC_NAME = "topic";

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @Mock
    TableflowTopicRepository tableflowTopicRepository;

    @Mock
    ConfluentCloudClient confluentCloudClient;

    @InjectMocks
    TableflowTopicAsyncExecutor tableflowTopicAsyncExecutor;

    @Test
    void shouldNotSynchronizeWhenTableflowIsNotManaged() {
        when(managedClusterProperties.isManageTableflow()).thenReturn(false);

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        verifyNoInteractions(tableflowTopicRepository, confluentCloudClient);
    }

    @Test
    void shouldCreateTableflowTopic() {
        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(TOPIC_NAME)
                        .namespace("ns1")
                        .cluster(CLUSTER)
                        .generation(0)
                        .updateTimestamp(Date.from(INSTANT))
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        TableflowTopicResponse tableflowTopicResponse = TableflowTopicResponse.builder()
                .id("tf-" + TOPIC_NAME)
                .spec(TableflowTopicResponse.Spec.builder()
                        .displayName(TOPIC_NAME)
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(new TableflowTopicResponse.Storage("Managed"))
                        .build())
                .status(new TableflowTopicResponse.Status("PENDING", null))
                .build();

        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(tableflowTopicRepository.findAllForCluster(CLUSTER)).thenReturn(List.of(tableflowTopic));
        when(tableflowTopicRepository.findByName(CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(tableflowTopic));
        when(confluentCloudClient.listTableflowTopics(CLUSTER)).thenReturn(Flux.empty());
        when(confluentCloudClient.createTableflowTopic(CLUSTER, tableflowTopic))
                .thenReturn(Mono.just(tableflowTopicResponse));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        assertTrue(tableflowTopic.isSuccess());
        assertEquals(1, tableflowTopic.getMetadata().getGeneration());
        verify(tableflowTopicRepository).create(tableflowTopic);
    }

    @Test
    void shouldMarkTableflowTopicAsSuccessWhenAlreadyExists() {
        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(TOPIC_NAME)
                        .namespace("ns1")
                        .cluster(CLUSTER)
                        .generation(0)
                        .updateTimestamp(Date.from(INSTANT))
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        TableflowTopicResponse tableflowTopicResponse = TableflowTopicResponse.builder()
                .id("tf-" + TOPIC_NAME)
                .spec(TableflowTopicResponse.Spec.builder()
                        .displayName(TOPIC_NAME)
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(new TableflowTopicResponse.Storage("Managed"))
                        .build())
                .status(new TableflowTopicResponse.Status("RUNNING", null))
                .build();

        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(tableflowTopicRepository.findAllForCluster(CLUSTER)).thenReturn(List.of(tableflowTopic));
        when(tableflowTopicRepository.findByName(CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(tableflowTopic));
        when(confluentCloudClient.listTableflowTopics(CLUSTER)).thenReturn(Flux.just(tableflowTopicResponse));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        assertTrue(tableflowTopic.isSuccess());
        assertEquals(1, tableflowTopic.getMetadata().getGeneration());
        verify(tableflowTopicRepository).create(tableflowTopic);
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldMarkTableflowTopicAsFailedWhenCreationFails() {
        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(TOPIC_NAME)
                        .namespace("ns1")
                        .cluster(CLUSTER)
                        .generation(0)
                        .updateTimestamp(Date.from(INSTANT))
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(tableflowTopicRepository.findAllForCluster(CLUSTER)).thenReturn(List.of(tableflowTopic));
        when(tableflowTopicRepository.findByName(CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(tableflowTopic));
        when(confluentCloudClient.listTableflowTopics(CLUSTER)).thenReturn(Flux.empty());
        when(confluentCloudClient.createTableflowTopic(CLUSTER, tableflowTopic))
                .thenReturn(Mono.error(new HttpClientResponseException(
                        "Internal error", HttpResponse.status(HttpStatus.SERVICE_UNAVAILABLE))));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        assertTrue(tableflowTopic.isFailed());
        assertEquals("Internal error", tableflowTopic.getMetadata().getStatus().getMessage());
        verify(tableflowTopicRepository).create(tableflowTopic);
    }

    @Test
    void shouldMarkTableflowTopicAsFailedWhenConfigurationDiffers() {
        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(TOPIC_NAME)
                        .namespace("ns1")
                        .cluster(CLUSTER)
                        .generation(0)
                        .updateTimestamp(Date.from(INSTANT))
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        TableflowTopicResponse tableflowTopicResponse = TableflowTopicResponse.builder()
                .id("tf-" + TOPIC_NAME)
                .spec(TableflowTopicResponse.Spec.builder()
                        .displayName(TOPIC_NAME)
                        .tableFormats(Set.of("DELTA"))
                        .storage(new TableflowTopicResponse.Storage("Managed"))
                        .build())
                .status(new TableflowTopicResponse.Status("RUNNING", null))
                .build();

        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(tableflowTopicRepository.findAllForCluster(CLUSTER)).thenReturn(List.of(tableflowTopic));
        when(tableflowTopicRepository.findByName(CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(tableflowTopic));
        when(confluentCloudClient.listTableflowTopics(CLUSTER)).thenReturn(Flux.just(tableflowTopicResponse));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        assertTrue(tableflowTopic.isFailed());
        assertEquals(
                "The existing Confluent Tableflow configuration differs from the requested configuration. Updates are not supported yet.",
                tableflowTopic.getMetadata().getStatus().getMessage());
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldNotSynchronizeWhenListingFails() {
        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(confluentCloudClient.listTableflowTopics(CLUSTER))
                .thenReturn(Flux.error(
                        new HttpClientResponseException("Forbidden", HttpResponse.status(HttpStatus.FORBIDDEN))));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        verifyNoInteractions(tableflowTopicRepository);
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldNotUpdateStatusWhenUnchanged() {
        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(TOPIC_NAME)
                        .namespace("ns1")
                        .cluster(CLUSTER)
                        .generation(1)
                        .updateTimestamp(Date.from(INSTANT))
                        .status(Resource.Metadata.Status.ofSuccess())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        TableflowTopicResponse tableflowTopicResponse = TableflowTopicResponse.builder()
                .id("tf-" + TOPIC_NAME)
                .spec(TableflowTopicResponse.Spec.builder()
                        .displayName(TOPIC_NAME)
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(new TableflowTopicResponse.Storage("Managed"))
                        .build())
                .status(new TableflowTopicResponse.Status("RUNNING", null))
                .build();

        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(tableflowTopicRepository.findAllForCluster(CLUSTER)).thenReturn(List.of(tableflowTopic));
        when(confluentCloudClient.listTableflowTopics(CLUSTER)).thenReturn(Flux.just(tableflowTopicResponse));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        assertEquals(1, tableflowTopic.getMetadata().getGeneration());
        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldNotUpdateStatusWhenDeletedDuringSynchronization() {
        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(TOPIC_NAME)
                        .namespace("ns1")
                        .cluster(CLUSTER)
                        .generation(0)
                        .updateTimestamp(Date.from(INSTANT))
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        TableflowTopicResponse tableflowTopicResponse = TableflowTopicResponse.builder()
                .id("tf-" + TOPIC_NAME)
                .spec(TableflowTopicResponse.Spec.builder()
                        .displayName(TOPIC_NAME)
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(new TableflowTopicResponse.Storage("Managed"))
                        .build())
                .status(new TableflowTopicResponse.Status("RUNNING", null))
                .build();

        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(tableflowTopicRepository.findAllForCluster(CLUSTER)).thenReturn(List.of(tableflowTopic));
        when(tableflowTopicRepository.findByName(CLUSTER, TOPIC_NAME)).thenReturn(Optional.empty());
        when(confluentCloudClient.listTableflowTopics(CLUSTER)).thenReturn(Flux.just(tableflowTopicResponse));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldNotUpdateStatusWhenReappliedDuringSynchronization() {
        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(TOPIC_NAME)
                        .namespace("ns1")
                        .cluster(CLUSTER)
                        .generation(0)
                        .updateTimestamp(Date.from(INSTANT))
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        TableflowTopic reappliedTableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(TOPIC_NAME)
                        .namespace("ns1")
                        .cluster(CLUSTER)
                        .generation(0)
                        .updateTimestamp(Date.from(INSTANT.plusSeconds(1)))
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        TableflowTopicResponse tableflowTopicResponse = TableflowTopicResponse.builder()
                .id("tf-" + TOPIC_NAME)
                .spec(TableflowTopicResponse.Spec.builder()
                        .displayName(TOPIC_NAME)
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(new TableflowTopicResponse.Storage("Managed"))
                        .build())
                .status(new TableflowTopicResponse.Status("RUNNING", null))
                .build();

        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(tableflowTopicRepository.findAllForCluster(CLUSTER)).thenReturn(List.of(tableflowTopic));
        when(tableflowTopicRepository.findByName(CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(reappliedTableflowTopic));
        when(confluentCloudClient.listTableflowTopics(CLUSTER)).thenReturn(Flux.just(tableflowTopicResponse));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldNotSynchronizeUnmanagedTableflowTopics() {
        TableflowTopicResponse tableflowTopicResponse = TableflowTopicResponse.builder()
                .id("tf-unmanaged")
                .spec(TableflowTopicResponse.Spec.builder()
                        .displayName("unmanaged")
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(new TableflowTopicResponse.Storage("Managed"))
                        .build())
                .status(new TableflowTopicResponse.Status("RUNNING", null))
                .build();

        when(managedClusterProperties.isManageTableflow()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(CLUSTER);
        when(tableflowTopicRepository.findAllForCluster(CLUSTER)).thenReturn(List.of());
        when(confluentCloudClient.listTableflowTopics(CLUSTER)).thenReturn(Flux.just(tableflowTopicResponse));

        StepVerifier.create(tableflowTopicAsyncExecutor.run()).verifyComplete();

        verify(tableflowTopicRepository, never()).create(any());
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }
}
