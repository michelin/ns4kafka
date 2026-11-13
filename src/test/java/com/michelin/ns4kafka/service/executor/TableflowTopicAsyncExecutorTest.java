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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.exceptions.HttpStatusException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class TableflowTopicAsyncExecutorTest {
    @Mock
    TableflowTopicRepository tableflowTopicRepository;

    @Mock
    ConfluentCloudClient confluentCloudClient;

    ManagedClusterProperties properties;
    TableflowTopicAsyncExecutor executor;
    TableflowTopic topic;

    @BeforeEach
    void setUp() {
        properties = new ManagedClusterProperties("cloud", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        executor = new TableflowTopicAsyncExecutor(properties, tableflowTopicRepository, confluentCloudClient);
        topic = topic("orders");
    }

    private static TableflowTopic topic(String name) {
        return TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(name)
                        .namespace("sales")
                        .cluster("cloud")
                        .generation(0)
                        .updateTimestamp(new Date(1000))
                        .status(Resource.Metadata.Status.ofPending())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(List.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();
    }

    private static TableflowTopicResponse response(String name, String phase) {
        return new TableflowTopicResponse(
                "tt-" + name,
                new TableflowTopicResponse.Spec(
                        name, List.of("ICEBERG"), new TableflowTopicResponse.Storage("Managed")),
                new TableflowTopicResponse.Status(phase, ""));
    }

    private void storedTopic() {
        when(tableflowTopicRepository.findAllForCluster("cloud")).thenReturn(List.of(topic));
        when(tableflowTopicRepository.findByName("cloud", "orders")).thenReturn(Optional.of(topic));
    }

    private void run() {
        executor.run().block(Duration.ofSeconds(5));
    }

    @Test
    void shouldSkipSelfManagedClusters() {
        properties.setProvider(ManagedClusterProperties.KafkaProvider.SELF_MANAGED);
        run();
        verifyNoInteractions(tableflowTopicRepository, confluentCloudClient);
    }

    @Test
    void shouldCreateAbsentTableflowAndKeepMaterializationPending() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.empty());
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenReturn(Mono.just(response("orders", "PENDING")));
        run();
        assertTrue(topic.isPending());
        assertEquals(0, topic.getMetadata().getGeneration());
        assertEquals("PENDING", topic.getStatus().getState().getPhase());
        verify(tableflowTopicRepository).create(topic);
    }

    @Test
    void shouldRecoverRunningTableflowFromStoredConfigurationAfterRestart() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(response("orders", "RUNNING")));
        run();
        assertTrue(topic.isSuccess());
        assertEquals(1, topic.getMetadata().getGeneration());
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldReadActualStateBeforeRetryingAfterCreationTimeout() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud"))
                .thenReturn(Flux.empty(), Flux.just(response("orders", "RUNNING")));
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenReturn(Mono.error(new HttpStatusException(HttpStatus.GATEWAY_TIMEOUT, "Creation timed out")));
        run();
        assertTrue(topic.isFailed());
        run();
        assertTrue(topic.isSuccess());
        verify(confluentCloudClient).createTableflowTopic("cloud", topic);
    }

    @Test
    void shouldRetryCreationOnTheNextRunWhenItWasNotAccepted() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.empty());
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenReturn(
                        Mono.error(new HttpClientResponseException(
                                "DO-NOT-EXPOSE", HttpResponse.status(HttpStatus.SERVICE_UNAVAILABLE))),
                        Mono.just(response("orders", "PENDING")));
        run();
        assertTrue(topic.isFailed());
        assertTrue(topic.getMetadata().getStatus().getMessage().contains("HTTP 503"));
        assertFalse(topic.getMetadata().getStatus().getMessage().contains("DO-NOT-EXPOSE"));
        run();
        assertTrue(topic.isPending());
        verify(confluentCloudClient, times(2)).createTableflowTopic("cloud", topic);
    }

    @Test
    void shouldNotCreateOrChangeStatusesWhenTheListingFails() {
        when(confluentCloudClient.listTableflowTopics("cloud"))
                .thenReturn(Flux.error(
                        new HttpClientResponseException("Unavailable", HttpResponse.status(HttpStatus.FORBIDDEN))));
        run();
        verifyNoInteractions(tableflowTopicRepository);
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldNotReconcileAPartialListingWhenALaterPageFails() {
        when(confluentCloudClient.listTableflowTopics("cloud"))
                .thenReturn(Flux.concat(
                        Flux.just(response("orders", "RUNNING")),
                        Flux.error(new IllegalStateException("Page failed"))));
        run();
        verifyNoInteractions(tableflowTopicRepository);
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldRecoverConcurrentCreationFromTheNextClusterListing() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud"))
                .thenReturn(Flux.empty(), Flux.just(response("orders", "RUNNING")));
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenReturn(Mono.error(
                        new HttpClientResponseException("Already exists", HttpResponse.status(HttpStatus.CONFLICT))));
        run();
        assertTrue(topic.isPending());
        run();
        assertTrue(topic.isSuccess());
        verify(confluentCloudClient).createTableflowTopic("cloud", topic);
        verify(confluentCloudClient, times(2)).listTableflowTopics("cloud");
    }

    @Test
    void shouldReportAnExistingConfigurationThatDoesNotMatch() {
        storedTopic();
        TableflowTopicResponse different = new TableflowTopicResponse(
                null,
                new TableflowTopicResponse.Spec(
                        "orders", List.of("DELTA"), new TableflowTopicResponse.Storage("Managed")),
                new TableflowTopicResponse.Status("RUNNING", ""));
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(different));
        run();
        assertTrue(topic.isFailed());
        assertTrue(topic.getMetadata().getStatus().getMessage().contains("differs"));
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldExposeConfluentMaterializationFailure() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(response("orders", "FAILED")));
        run();
        assertTrue(topic.isFailed());
        assertEquals("FAILED", topic.getStatus().getState().getPhase());
        assertEquals(0, topic.getMetadata().getGeneration());
    }

    @Test
    void shouldPollPendingMaterializationUntilRunning() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud"))
                .thenReturn(Flux.just(response("orders", "PENDING")), Flux.just(response("orders", "RUNNING")));
        run();
        assertTrue(topic.isPending());
        run();
        assertTrue(topic.isSuccess());
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldNotRewriteAnUnchangedSuccessfulStatus() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(response("orders", "RUNNING")));
        run();
        run();
        assertEquals(1, topic.getMetadata().getGeneration());
        verify(tableflowTopicRepository).create(topic);
    }

    @Test
    void shouldNotRestoreAResourceDeletedDuringSynchronization() {
        storedTopic();
        when(tableflowTopicRepository.findByName("cloud", "orders")).thenReturn(Optional.empty());
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(response("orders", "RUNNING")));
        run();
        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldNotOverwriteAResourceReappliedDuringSynchronization() {
        storedTopic();
        TableflowTopic newer = topic("orders");
        newer.getMetadata().setUpdateTimestamp(new Date(2000));
        when(tableflowTopicRepository.findByName("cloud", "orders")).thenReturn(Optional.of(newer));
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(response("orders", "RUNNING")));
        run();
        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldContinueWithOtherResourcesAndSanitizeUnexpectedErrors() {
        storedTopic();
        TableflowTopic next = topic("next");
        when(tableflowTopicRepository.findAllForCluster("cloud")).thenReturn(List.of(topic, next));
        when(tableflowTopicRepository.findByName("cloud", "next")).thenReturn(Optional.of(next));
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(response("next", "RUNNING")));
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenReturn(Mono.error(new IllegalStateException("DO-NOT-EXPOSE")));
        run();
        assertTrue(topic.isFailed());
        assertFalse(topic.getMetadata().getStatus().getMessage().contains("DO-NOT-EXPOSE"));
        assertTrue(next.isSuccess());
    }

    @Test
    void shouldContinueWithOtherResourcesWhenSavingStatusFails() {
        storedTopic();
        TableflowTopic next = topic("next");
        when(tableflowTopicRepository.findAllForCluster("cloud")).thenReturn(List.of(topic, next));
        when(tableflowTopicRepository.findByName("cloud", "next")).thenReturn(Optional.of(next));
        when(confluentCloudClient.listTableflowTopics("cloud"))
                .thenReturn(Flux.just(response("orders", "RUNNING"), response("next", "RUNNING")));
        when(tableflowTopicRepository.create(topic)).thenThrow(new IllegalStateException("Storage unavailable"));
        run();
        assertTrue(next.isSuccess());
        verify(tableflowTopicRepository).create(next);
    }

    @Test
    void shouldRetryAnUnpersistedFailureStatusOnTheNextRun() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.empty());
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenReturn(Mono.error(
                        new HttpClientResponseException("Forbidden", HttpResponse.status(HttpStatus.FORBIDDEN))));
        when(tableflowTopicRepository.create(topic))
                .thenThrow(new KafkaStoreException("Storage unavailable"))
                .thenReturn(topic);

        run();
        assertTrue(topic.isPending());
        run();

        assertTrue(topic.isFailed());
        verify(tableflowTopicRepository, times(2)).create(topic);
    }

    @Test
    void shouldAdvanceGenerationOnlyAfterTheSuccessStatusIsPersisted() {
        storedTopic();
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(response("orders", "RUNNING")));
        when(tableflowTopicRepository.create(topic))
                .thenThrow(new KafkaStoreException("Storage unavailable"))
                .thenReturn(topic);

        run();
        assertTrue(topic.isPending());
        assertEquals(0, topic.getMetadata().getGeneration());
        run();

        assertTrue(topic.isSuccess());
        assertEquals(1, topic.getMetadata().getGeneration());
        verify(tableflowTopicRepository, times(2)).create(topic);
    }

    @Test
    void shouldReconcileHundredsOfTopicsWithOneClusterListing() {
        List<TableflowTopic> topics = IntStream.range(0, 300)
                .mapToObj(index -> topic("orders-" + index))
                .toList();
        when(tableflowTopicRepository.findAllForCluster("cloud")).thenReturn(topics);
        when(tableflowTopicRepository.findByName(eq("cloud"), anyString()))
                .thenAnswer(invocation -> topics.stream()
                        .filter(current -> current.getMetadata().getName().equals(invocation.getArgument(1)))
                        .findFirst());
        when(confluentCloudClient.listTableflowTopics("cloud"))
                .thenReturn(Flux.fromIterable(topics)
                        .map(current -> response(current.getMetadata().getName(), "RUNNING")));
        run();
        assertTrue(topics.stream().allMatch(Resource::isSuccess));
        verify(confluentCloudClient).listTableflowTopics("cloud");
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldLeaveUnmanagedRemoteTopicsAlone() {
        when(tableflowTopicRepository.findAllForCluster("cloud")).thenReturn(List.of());
        when(confluentCloudClient.listTableflowTopics("cloud")).thenReturn(Flux.just(response("unmanaged", "RUNNING")));
        run();
        verify(tableflowTopicRepository, never()).create(any());
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }
}
