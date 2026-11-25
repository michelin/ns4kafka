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
import com.michelin.ns4kafka.service.client.confluent.entities.ConfluentCloudErrorResponse;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Tableflow topic executor. */
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
     * Run the Tableflow topic synchronization.
     *
     * @return A mono completing when synchronization is done
     */
    public Mono<Void> run() {
        if (!managedClusterProperties.isManageTableflow()) {
            return Mono.empty();
        }
        return synchronizeTableflowTopics();
    }

    /**
     * Synchronize the Tableflow topics.
     *
     * @return A mono completing when synchronization is done
     */
    public Mono<Void> synchronizeTableflowTopics() {
        log.debug("Starting Tableflow topic collection for cluster {}.", managedClusterProperties.getName());

        return Mono.defer(() -> confluentCloudClient
                        .listTableflowTopics(managedClusterProperties.getName())
                        .collectMap(response -> response.spec().displayName()))
                .publishOn(Schedulers.boundedElastic())
                .flatMap(brokerTableflowTopics -> {
                    List<TableflowTopic> ns4KafkaTableflowTopics =
                            tableflowTopicRepository.findAllForCluster(managedClusterProperties.getName());

                    return reconcileTableflowTopics(ns4KafkaTableflowTopics, brokerTableflowTopics)
                            .publishOn(Schedulers.boundedElastic())
                            .doOnNext(errors -> updateStatuses(ns4KafkaTableflowTopics, errors));
                })
                .then()
                .doOnError(exception -> log.error(
                        "An error occurred during the Tableflow topic synchronization on cluster {}.",
                        managedClusterProperties.getName(),
                        exception))
                .onErrorComplete();
    }

    /**
     * Reconcile Tableflow topics against the broker configuration.
     *
     * @param tableflowTopics The desired Tableflow topics
     * @param brokerTableflowTopics The Confluent Cloud Tableflow topics by name
     * @return The reconciliation errors by topic name
     */
    private Mono<Map<String, String>> reconcileTableflowTopics(
            List<TableflowTopic> tableflowTopics, Map<String, TableflowTopicResponse> brokerTableflowTopics) {
        return Mono.defer(() -> {
            Map<String, String> errors = new ConcurrentHashMap<>();
            String cluster = managedClusterProperties.getName();
            return Flux.fromIterable(tableflowTopics)
                    .concatMap(tableflowTopic -> {
                        String name = tableflowTopic.getMetadata().getName();
                        TableflowTopicResponse observed = brokerTableflowTopics.get(name);
                        Long retentionMs = tableflowTopic.getSpec().getSnapshotRetentionMs();
                        if (observed != null && !matchesTableLayout(tableflowTopic, observed)) {
                            errors.put(
                                    name,
                                    "The existing Confluent Tableflow configuration differs in table formats or storage kind.");
                            return Mono.empty();
                        }
                        if (observed != null
                                && (retentionMs == null
                                        || (observed.spec().config() != null
                                                && retentionMs
                                                        .toString()
                                                        .equals(observed.spec()
                                                                .config()
                                                                .retentionMs())))) {
                            return Mono.empty();
                        }

                        String action = observed == null ? "creating" : "updating";
                        log.debug("Start {} Tableflow topic {} on cluster {}.", action, name, cluster);
                        return Mono.defer(() -> observed == null
                                        ? confluentCloudClient.createTableflowTopic(cluster, tableflowTopic)
                                        : confluentCloudClient.updateTableflowTopic(cluster, name, retentionMs))
                                .doOnNext(_ ->
                                        log.info("Success {} Tableflow topic {} on cluster {}.", action, name, cluster))
                                .onErrorResume(e -> {
                                    String errorMessage = e instanceof HttpClientResponseException responseException
                                            ? responseException
                                                    .getResponse()
                                                    .getBody(ConfluentCloudErrorResponse.class)
                                                    .filter(error -> error.errors() != null)
                                                    .map(error -> error.errors().stream()
                                                            .map(ConfluentCloudErrorResponse.Error::detail)
                                                            .collect(Collectors.joining(", ")))
                                                    .orElse(responseException.getMessage())
                                            : Objects.toString(
                                                    e.getMessage(), e.getClass().getName());

                                    log.error(
                                            "Error while {} Tableflow topic {} on cluster {}.",
                                            action,
                                            name,
                                            cluster,
                                            e);

                                    errors.put(name, errorMessage);
                                    return Mono.empty();
                                });
                    })
                    .then(Mono.just(errors));
        });
    }

    /**
     * Update the status of the Tableflow topics.
     *
     * @param ns4KafkaTableflowTopics The Ns4Kafka Tableflow topics
     * @param errors The reconciliation errors by topic name
     */
    private void updateStatuses(List<TableflowTopic> ns4KafkaTableflowTopics, Map<String, String> errors) {
        ns4KafkaTableflowTopics.forEach(tableflowTopic -> {
            String error = errors.get(tableflowTopic.getMetadata().getName());
            Resource.Metadata.Status status =
                    error == null ? Resource.Metadata.Status.ofSuccess() : Resource.Metadata.Status.ofFailed(error);

            if (Objects.equals(tableflowTopic.getMetadata().getStatus(), status)) {
                return;
            }

            // Do not overwrite a Tableflow topic deleted or reapplied since it was read
            if (!isUnchangedSinceLastApply(tableflowTopic)) {
                return;
            }

            if (status.getPhase() == Resource.Metadata.Phase.SUCCESS) {
                tableflowTopic
                        .getMetadata()
                        .setGeneration(tableflowTopic.getMetadata().getGeneration() + 1);
            }

            tableflowTopic.getMetadata().setStatus(status);
            tableflowTopicRepository.create(tableflowTopic);
        });
    }

    /**
     * Check if a Tableflow topic matches its Confluent Cloud formats and storage.
     *
     * @param tableflowTopic The Tableflow topic
     * @param response The Confluent Cloud Tableflow topic
     * @return True if they match, false otherwise
     */
    private static boolean matchesTableLayout(TableflowTopic tableflowTopic, TableflowTopicResponse response) {
        TableflowTopic.Storage desiredStorage = tableflowTopic.getSpec().getStorage();
        TableflowTopicResponse.Storage observedStorage = response.spec().storage();
        return Objects.equals(
                        tableflowTopic.getSpec().getTableFormats(),
                        response.spec().tableFormats())
                && Objects.equals(
                        desiredStorage == null ? null : desiredStorage.getKind(),
                        observedStorage == null ? null : observedStorage.kind());
    }

    /**
     * Check the Tableflow topic has been neither deleted nor reapplied since it was read.
     *
     * @param tableflowTopic The synchronized Tableflow topic
     * @return True if unchanged, false otherwise
     */
    private boolean isUnchangedSinceLastApply(TableflowTopic tableflowTopic) {
        return tableflowTopicRepository
                .findByName(
                        managedClusterProperties.getName(),
                        tableflowTopic.getMetadata().getName())
                .map(existingTableflowTopic -> existingTableflowTopic.isOlderOrEqualTo(tableflowTopic))
                .orElse(false);
    }
}
