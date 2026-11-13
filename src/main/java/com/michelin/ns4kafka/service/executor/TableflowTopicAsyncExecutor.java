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

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Singleton;
import java.util.HashSet;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Deploy stored Tableflow configurations and recover uncertain creation outcomes. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class TableflowTopicAsyncExecutor {
    private final ManagedClusterProperties managedClusterProperties;
    private final TableflowTopicRepository tableflowTopicRepository;
    private final ConfluentCloudClient confluentCloudClient;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param tableflowTopicRepository The Tableflow topic repository
     * @param confluentCloudClient The Confluent Cloud client
     */
    public TableflowTopicAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            TableflowTopicRepository tableflowTopicRepository,
            ConfluentCloudClient confluentCloudClient) {
        this.managedClusterProperties = managedClusterProperties;
        this.tableflowTopicRepository = tableflowTopicRepository;
        this.confluentCloudClient = confluentCloudClient;
    }

    /**
     * Run Tableflow synchronization for a Confluent Cloud cluster.
     *
     * @return A mono completing when synchronization is done
     */
    public Mono<Void> run() {
        if (!managedClusterProperties.isConfluentCloud()) {
            return Mono.empty();
        }
        return synchronizeTableflowTopics();
    }

    /**
     * Collect all remote topics before reconciling the configurations stored in Ns4Kafka.
     *
     * @return A mono completing when synchronization is done
     */
    public Mono<Void> synchronizeTableflowTopics() {
        log.debug("Starting Tableflow topic collection for cluster {}.", managedClusterProperties.getName());
        return Mono.defer(() -> confluentCloudClient
                        .listTableflowTopics(managedClusterProperties.getName())
                        .collectMap(response -> response.spec().displayName()))
                // Ns4Kafka storage is blocking; keep it outside the HTTP client event loop.
                .publishOn(Schedulers.boundedElastic())
                .flatMap(brokerTopics -> Flux.fromIterable(
                                tableflowTopicRepository.findAllForCluster(managedClusterProperties.getName()))
                        .concatMap(topic -> synchronizeTableflowTopic(
                                        topic,
                                        brokerTopics.get(topic.getMetadata().getName()))
                                .doOnError(_ -> log.error(
                                        "Could not persist Tableflow status for topic {} on cluster {}.",
                                        topic.getMetadata().getName(),
                                        managedClusterProperties.getName()))
                                .onErrorComplete())
                        .then())
                .doOnError(_ -> log.error(
                        "Could not collect Tableflow topics for cluster {}. Synchronization will be retried.",
                        managedClusterProperties.getName()))
                .onErrorComplete();
    }

    /**
     * Create a missing configuration or update its materialization status from the cluster snapshot.
     *
     * @param topic The stored Tableflow configuration
     * @param existing The remote configuration, or null if absent from the complete listing
     * @return A mono completing when the resource status has been saved
     */
    private Mono<Void> synchronizeTableflowTopic(TableflowTopic topic, @Nullable TableflowTopicResponse existing) {
        return Mono.defer(() -> existing != null ? Mono.just(existing) : createTableflowTopic(topic))
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(response -> {
                    if (!matchesConfiguration(topic, response)) {
                        saveStatus(
                                topic,
                                topic.getStatus(),
                                Resource.Metadata.Status.ofFailed(
                                        "The existing Confluent Tableflow configuration differs from the requested configuration. Updates are not supported yet."));
                        return;
                    }
                    updateStatus(topic, response);
                })
                .then()
                .onErrorResume(
                        exception -> !(exception instanceof KafkaStoreException),
                        exception -> Mono.fromRunnable(() -> {
                            if (exception instanceof HttpClientResponseException responseException
                                    && responseException.getStatus() == HttpStatus.CONFLICT) {
                                saveStatus(topic, topic.getStatus(), Resource.Metadata.Status.ofPending());
                                return;
                            }
                            String message = exception instanceof HttpClientResponseException responseException
                                    ? "Confluent Tableflow creation returned HTTP " + responseException.code()
                                            + ". Check the Confluent Cloud credentials, permissions, and topic configuration."
                                    : "Tableflow synchronization failed. Check the cluster configuration and Confluent availability.";
                            log.error(
                                    "Tableflow synchronization failed for topic {} on cluster {}.",
                                    topic.getMetadata().getName(),
                                    managedClusterProperties.getName());
                            saveStatus(topic, topic.getStatus(), Resource.Metadata.Status.ofFailed(message));
                        }));
    }

    /**
     * Submit creation once; the next cluster listing recovers conflicts or uncertain outcomes.
     *
     * @param topic The stored Tableflow configuration
     * @return The Confluent resource
     */
    private Mono<TableflowTopicResponse> createTableflowTopic(TableflowTopic topic) {
        log.debug(
                "Creating Tableflow topic {} on cluster {}.",
                topic.getMetadata().getName(),
                managedClusterProperties.getName());
        return confluentCloudClient
                .createTableflowTopic(managedClusterProperties.getName(), topic)
                .switchIfEmpty(Mono.error(new IllegalStateException("Empty Tableflow creation response")))
                .doOnNext(_ -> log.info(
                        "Success creating Tableflow topic {} on cluster {}.",
                        topic.getMetadata().getName(),
                        managedClusterProperties.getName()));
    }

    /**
     * Compare only the settings managed by Ns4Kafka, ignoring Confluent defaults.
     *
     * @param topic The desired configuration
     * @param response The Confluent resource
     * @return Whether both configurations match
     */
    private static boolean matchesConfiguration(TableflowTopic topic, TableflowTopicResponse response) {
        TableflowTopicResponse.Spec spec = response.spec();
        if (spec == null || spec.storage() == null || spec.tableFormats() == null) {
            throw new IllegalStateException("Incomplete Confluent Tableflow response");
        }
        return new HashSet<>(spec.tableFormats())
                        .equals(new HashSet<>(topic.getSpec().getTableFormats()))
                && topic.getSpec().getStorage().getKind().equals(spec.storage().kind());
    }

    /**
     * Persist materialization progress separately from the desired configuration.
     *
     * @param topic The stored configuration
     * @param response The Confluent resource
     */
    private void updateStatus(TableflowTopic topic, TableflowTopicResponse response) {
        TableflowTopicResponse.Status state = response.status();
        String phase = state == null || state.phase() == null ? "PENDING" : state.phase();
        Resource.Metadata.Status status =
                switch (phase) {
                    case "RUNNING" -> Resource.Metadata.Status.ofSuccess();
                    case "FAILED" ->
                        Resource.Metadata.Status.ofFailed(
                                "Confluent Tableflow materialization failed. Check the topic in Confluent Cloud.");
                    default ->
                        Resource.Metadata.Status.builder()
                                .phase(Resource.Metadata.Phase.PENDING)
                                .message("Creation accepted by Confluent; waiting for Tableflow to reach RUNNING.")
                                .build();
                };
        saveStatus(
                topic,
                TableflowTopic.TableflowTopicStatus.builder()
                        .id(response.id())
                        .state(
                                state == null
                                        ? null
                                        : TableflowTopic.TableflowTopicState.builder()
                                                .phase(state.phase())
                                                .errorMessage(state.errorMessage())
                                                .build())
                        .build(),
                status);
    }

    /**
     * Save a status only when it changed and the resource was not removed or reapplied.
     *
     * @param topic The resource read before synchronization
     * @param state The Confluent status
     * @param status The deployment status
     */
    private void saveStatus(
            TableflowTopic topic, TableflowTopic.TableflowTopicStatus state, Resource.Metadata.Status status) {
        boolean unchanged = tableflowTopicRepository
                .findByName(
                        managedClusterProperties.getName(), topic.getMetadata().getName())
                .map(current ->
                        current.isOlderOrEqualTo(topic) && current.getSpec().equals(topic.getSpec()))
                .orElse(false);
        if (!unchanged
                || (Objects.equals(topic.getStatus(), state)
                        && Objects.equals(topic.getMetadata().getStatus(), status))) {
            return;
        }
        TableflowTopic.TableflowTopicStatus previousState = topic.getStatus();
        Resource.Metadata.Status previousStatus = topic.getMetadata().getStatus();
        int previousGeneration = topic.getMetadata().getGeneration();
        if (status.getPhase() == Resource.Metadata.Phase.SUCCESS && !topic.isSuccess()) {
            topic.getMetadata().setGeneration(previousGeneration + 1);
        }
        topic.setStatus(state);
        topic.getMetadata().setStatus(status);
        try {
            tableflowTopicRepository.create(topic);
        } catch (KafkaStoreException exception) {
            // The repository returns its cached resource; retain the previous state so the next run retries the write.
            topic.setStatus(previousState);
            topic.getMetadata().setStatus(previousStatus);
            topic.getMetadata().setGeneration(previousGeneration);
            throw exception;
        }
    }
}
