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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.json.tree.JsonNode;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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

    private static JsonNode response(String phase) {
        return JsonNode.from(Map.of(
                "id",
                "tt-test",
                "spec",
                Map.of("table_formats", List.of("ICEBERG"), "storage", Map.of("kind", "Managed")),
                "status",
                Map.of("phase", phase)));
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
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.empty());
        when(confluentCloudClient.createTableflowTopic("cloud", topic)).thenReturn(response("PENDING"));
        run();
        assertTrue(topic.isPending());
        assertEquals(0, topic.getMetadata().getGeneration());
        assertEquals("PENDING", topic.getStatus().getState().get("phase").getStringValue());
        verify(tableflowTopicRepository).create(topic);
    }

    @Test
    void shouldRecoverRunningTableflowFromStoredConfigurationAfterRestart() {
        storedTopic();
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.of(response("RUNNING")));
        run();
        assertTrue(topic.isSuccess());
        assertEquals(1, topic.getMetadata().getGeneration());
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldReadActualStateBeforeRetryingAfterCreationTimeout() {
        storedTopic();
        when(confluentCloudClient.getTableflowTopic("cloud", "orders"))
                .thenReturn(Optional.empty(), Optional.of(response("RUNNING")));
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenThrow(new HttpStatusException(HttpStatus.GATEWAY_TIMEOUT, "Creation timed out"));
        run();
        assertTrue(topic.isFailed());
        run();
        assertTrue(topic.isSuccess());
        verify(confluentCloudClient, times(1)).createTableflowTopic("cloud", topic);
    }

    @Test
    void shouldRetryCreationOnTheNextRunWhenItWasNotAccepted() {
        storedTopic();
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.empty());
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenThrow(new HttpStatusException(HttpStatus.BAD_GATEWAY, "Confluent unavailable"))
                .thenReturn(response("PENDING"));
        run();
        assertTrue(topic.isFailed());
        run();
        assertTrue(topic.isPending());
        verify(confluentCloudClient, times(2)).createTableflowTopic("cloud", topic);
    }

    @Test
    void shouldNotCreateWhenTheLookupFails() {
        storedTopic();
        when(confluentCloudClient.getTableflowTopic("cloud", "orders"))
                .thenThrow(new HttpStatusException(HttpStatus.BAD_GATEWAY, "Lookup returned HTTP 403"));
        run();
        assertTrue(topic.isFailed());
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldResolveConcurrentCreationThroughAReadAfterConflict() {
        storedTopic();
        when(confluentCloudClient.getTableflowTopic("cloud", "orders"))
                .thenReturn(Optional.empty(), Optional.of(response("RUNNING")));
        when(confluentCloudClient.createTableflowTopic("cloud", topic))
                .thenThrow(new HttpStatusException(HttpStatus.CONFLICT, "Already exists"));
        run();
        assertTrue(topic.isSuccess());
        verify(confluentCloudClient, times(2)).getTableflowTopic("cloud", "orders");
    }

    @Test
    void shouldReportAnExistingConfigurationThatDoesNotMatch() {
        storedTopic();
        JsonNode different = JsonNode.from(Map.of(
                "spec",
                Map.of("table_formats", List.of("DELTA"), "storage", Map.of("kind", "Managed")),
                "status",
                Map.of("phase", "RUNNING")));
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.of(different));
        run();
        assertTrue(topic.isFailed());
        assertTrue(topic.getMetadata().getStatus().getMessage().contains("differs"));
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldExposeConfluentMaterializationFailure() {
        storedTopic();
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.of(response("FAILED")));
        run();
        assertTrue(topic.isFailed());
        assertEquals("FAILED", topic.getStatus().getState().get("phase").getStringValue());
        assertEquals(0, topic.getMetadata().getGeneration());
    }

    @Test
    void shouldPollPendingMaterializationUntilRunning() {
        storedTopic();
        when(confluentCloudClient.getTableflowTopic("cloud", "orders"))
                .thenReturn(Optional.of(response("PENDING")), Optional.of(response("RUNNING")));
        run();
        assertTrue(topic.isPending());
        run();
        assertTrue(topic.isSuccess());
        verify(confluentCloudClient, never()).createTableflowTopic(any(), any());
    }

    @Test
    void shouldNotRewriteAnUnchangedSuccessfulStatus() {
        storedTopic();
        JsonNode running = response("RUNNING");
        topic.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
        topic.getMetadata().setGeneration(1);
        topic.setStatus(TableflowTopic.TableflowTopicStatus.builder()
                .id("tt-test")
                .state(running.get("status"))
                .build());
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.of(running));
        run();
        assertEquals(1, topic.getMetadata().getGeneration());
        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldNotRestoreAResourceDeletedDuringSynchronization() {
        storedTopic();
        when(tableflowTopicRepository.findByName("cloud", "orders")).thenReturn(Optional.empty());
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.of(response("RUNNING")));
        run();
        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldNotOverwriteAResourceReappliedDuringSynchronization() {
        storedTopic();
        TableflowTopic newer = topic("orders");
        newer.getMetadata().setUpdateTimestamp(new Date(2000));
        when(tableflowTopicRepository.findByName("cloud", "orders")).thenReturn(Optional.of(newer));
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.of(response("RUNNING")));
        run();
        verify(tableflowTopicRepository, never()).create(any());
    }

    @Test
    void shouldContinueWithOtherResourcesAndSanitizeUnexpectedErrors() {
        storedTopic();
        TableflowTopic next = topic("next");
        when(tableflowTopicRepository.findAllForCluster("cloud")).thenReturn(List.of(topic, next));
        when(tableflowTopicRepository.findByName("cloud", "next")).thenReturn(Optional.of(next));
        when(confluentCloudClient.getTableflowTopic("cloud", "orders"))
                .thenThrow(new IllegalStateException("DO-NOT-EXPOSE"));
        when(confluentCloudClient.getTableflowTopic("cloud", "next")).thenReturn(Optional.of(response("RUNNING")));
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
        when(confluentCloudClient.getTableflowTopic("cloud", "orders")).thenReturn(Optional.of(response("RUNNING")));
        when(confluentCloudClient.getTableflowTopic("cloud", "next")).thenReturn(Optional.of(response("RUNNING")));
        when(tableflowTopicRepository.create(topic)).thenThrow(new IllegalStateException("Storage unavailable"));

        run();

        assertTrue(next.isSuccess());
        verify(tableflowTopicRepository).create(next);
    }
}
