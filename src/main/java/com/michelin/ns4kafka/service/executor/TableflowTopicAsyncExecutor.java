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
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.json.tree.JsonNode;
import jakarta.inject.Singleton;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
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
     * Synchronize stored resources outside the HTTP client event loop.
     *
     * @return A mono completing when synchronization is done
     */
    public Mono<Void> synchronizeTableflowTopics() {
        return Mono.defer(() -> Flux.fromIterable(
                                tableflowTopicRepository.findAllForCluster(managedClusterProperties.getName()))
                        .concatMap(topic -> Mono.fromRunnable(() -> synchronizeTableflowTopic(topic))
                                .doOnError(_ -> log.error(
                                        "Could not persist Tableflow status for topic {} on cluster {}.",
                                        topic.getMetadata().getName(),
                                        managedClusterProperties.getName()))
                                .onErrorComplete())
                        .then())
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Read the actual state before creating, including after a timeout or restart.
     *
     * @param topic The stored Tableflow configuration
     */
    private void synchronizeTableflowTopic(TableflowTopic topic) {
        try {
            Optional<JsonNode> existing = confluentCloudClient.getTableflowTopic(
                    managedClusterProperties.getName(), topic.getMetadata().getName());
            JsonNode response = existing.isPresent() ? existing.get() : createTableflowTopic(topic);
            if (!matchesConfiguration(topic, response)) {
                saveStatus(
                        topic,
                        topic.getStatus(),
                        Resource.Metadata.Status.ofFailed(
                                "The existing Confluent Tableflow configuration differs from the requested configuration. Updates are not supported yet."));
                return;
            }
            updateStatus(topic, response);
        } catch (RuntimeException exception) {
            String message = exception instanceof HttpStatusException
                    ? exception.getMessage()
                    : "Tableflow synchronization failed. Check the cluster configuration and Confluent availability.";
            log.error(
                    "Tableflow synchronization failed for topic {} on cluster {}.",
                    topic.getMetadata().getName(),
                    managedClusterProperties.getName());
            saveStatus(topic, topic.getStatus(), Resource.Metadata.Status.ofFailed(message));
        }
    }

    /**
     * Submit creation once, reading the actual resource if another request already created it.
     *
     * @param topic The stored Tableflow configuration
     * @return The Confluent resource
     */
    private JsonNode createTableflowTopic(TableflowTopic topic) {
        try {
            return confluentCloudClient.createTableflowTopic(managedClusterProperties.getName(), topic);
        } catch (HttpStatusException exception) {
            if (exception.getStatus() != HttpStatus.CONFLICT) {
                throw exception;
            }
            return confluentCloudClient
                    .getTableflowTopic(
                            managedClusterProperties.getName(),
                            topic.getMetadata().getName())
                    .orElseThrow(() -> exception);
        }
    }

    /**
     * Compare only the settings managed by Ns4Kafka, ignoring Confluent defaults.
     *
     * @param topic The desired configuration
     * @param response The Confluent resource
     * @return Whether both configurations match
     */
    private static boolean matchesConfiguration(TableflowTopic topic, JsonNode response) {
        JsonNode spec = response.get("spec");
        if (spec == null || spec.get("storage") == null || spec.get("table_formats") == null) {
            throw new IllegalStateException("Incomplete Confluent Tableflow response");
        }
        var formats = new HashSet<String>();
        spec.get("table_formats").values().forEach(format -> formats.add(format.getStringValue()));
        return formats.equals(new HashSet<>(topic.getSpec().getTableFormats()))
                && topic.getSpec()
                        .getStorage()
                        .getKind()
                        .equals(spec.get("storage").get("kind").getStringValue());
    }

    /**
     * Persist materialization progress separately from the desired configuration.
     *
     * @param topic The stored configuration
     * @param response The Confluent resource
     */
    private void updateStatus(TableflowTopic topic, JsonNode response) {
        JsonNode state = response.get("status");
        String phase = state == null || state.get("phase") == null
                ? "PENDING"
                : state.get("phase").getStringValue();
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
                        .id(
                                response.get("id") == null
                                        ? null
                                        : response.get("id").getStringValue())
                        .state(state)
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
        if (status.getPhase() == Resource.Metadata.Phase.SUCCESS && !topic.isSuccess()) {
            topic.getMetadata().setGeneration(topic.getMetadata().getGeneration() + 1);
        }
        topic.setStatus(state);
        topic.getMetadata().setStatus(status);
        tableflowTopicRepository.create(topic);
    }
}
