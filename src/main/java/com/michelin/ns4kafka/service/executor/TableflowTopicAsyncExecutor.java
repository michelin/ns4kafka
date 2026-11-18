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
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Locale;
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
                .flatMap(brokerTableflowTopics -> {
                    List<TableflowTopic> ns4KafkaTableflowTopics =
                            tableflowTopicRepository.findAllForCluster(managedClusterProperties.getName());

                    List<TableflowTopic> toCreate = ns4KafkaTableflowTopics.stream()
                            .filter(tableflowTopic -> !brokerTableflowTopics.containsKey(
                                    tableflowTopic.getMetadata().getName()))
                            .toList();

                    if (!toCreate.isEmpty()) {
                        log.atDebug()
                                .addArgument(() -> toCreate.stream()
                                        .map(tableflowTopic ->
                                                tableflowTopic.getMetadata().getName())
                                        .collect(Collectors.joining(",")))
                                .log("Tableflow topic(s) to create: {}.");
                    }

                    return createTableflowTopics(toCreate)
                            .publishOn(Schedulers.boundedElastic())
                            .doOnNext(creationErrors ->
                                    updateStatuses(ns4KafkaTableflowTopics, brokerTableflowTopics, creationErrors));
                })
                .then()
                .doOnError(exception -> log.error(
                        "An error occurred during the Tableflow topic synchronization on cluster {}.",
                        managedClusterProperties.getName(),
                        exception))
                .onErrorComplete();
    }

    /**
     * Create Tableflow topics.
     *
     * @param toCreate The Tableflow topics to create
     * @return The error message of each Tableflow topic that could not be created, by topic name
     */
    Mono<Map<String, String>> createTableflowTopics(List<TableflowTopic> toCreate) {
        return Mono.defer(() -> {
            Map<String, String> creationErrors = new ConcurrentHashMap<>();

            return Flux.fromIterable(toCreate)
                    .concatMap(tableflowTopic -> confluentCloudClient
                            .createTableflowTopic(managedClusterProperties.getName(), tableflowTopic)
                            .doOnNext(_ -> log.info(
                                    "Success creating Tableflow topic {} on cluster {}.",
                                    tableflowTopic.getMetadata().getName(),
                                    managedClusterProperties.getName()))
                            .onErrorResume(e -> {
                                log.error(
                                        "Error while creating Tableflow topic {} on cluster {}.",
                                        tableflowTopic.getMetadata().getName(),
                                        managedClusterProperties.getName(),
                                        e);

                                creationErrors.put(
                                        tableflowTopic.getMetadata().getName(),
                                        Objects.toString(
                                                e.getMessage(), e.getClass().getName()));
                                return Mono.empty();
                            }))
                    .then(Mono.fromSupplier(() -> creationErrors));
        });
    }

    /**
     * Update the status of the Tableflow topics.
     *
     * @param ns4KafkaTableflowTopics The Ns4Kafka Tableflow topics
     * @param brokerTableflowTopics The Confluent Cloud Tableflow topics by name
     * @param creationErrors The error message of each Tableflow topic that could not be created, by topic name
     */
    private void updateStatuses(
            List<TableflowTopic> ns4KafkaTableflowTopics,
            Map<String, TableflowTopicResponse> brokerTableflowTopics,
            Map<String, String> creationErrors) {
        ns4KafkaTableflowTopics.forEach(tableflowTopic -> {
            TableflowTopicResponse brokerTableflowTopic =
                    brokerTableflowTopics.get(tableflowTopic.getMetadata().getName());

            Resource.Metadata.Status status;
            if (creationErrors.containsKey(tableflowTopic.getMetadata().getName())) {
                status = Resource.Metadata.Status.ofFailed(
                        creationErrors.get(tableflowTopic.getMetadata().getName()));
            } else if (brokerTableflowTopic != null && !matchesConfiguration(tableflowTopic, brokerTableflowTopic)) {
                status = Resource.Metadata.Status.ofFailed(
                        "The existing Confluent Tableflow configuration differs from the requested configuration. Updates are not supported yet.");
            } else {
                status = Resource.Metadata.Status.ofSuccess();
            }

            // Status is up to date
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
     * Check if a Tableflow topic matches its Confluent Cloud configuration, ignoring case.
     *
     * @param tableflowTopic The Tableflow topic
     * @param response The Confluent Cloud Tableflow topic
     * @return true if they match, false otherwise
     */
    private static boolean matchesConfiguration(TableflowTopic tableflowTopic, TableflowTopicResponse response) {
        return response.spec().tableFormats().stream()
                        .map(tableFormat -> tableFormat.toUpperCase(Locale.ROOT))
                        .collect(Collectors.toSet())
                        .equals(tableflowTopic.getSpec().getTableFormats().stream()
                                .map(tableFormat -> tableFormat.toUpperCase(Locale.ROOT))
                                .collect(Collectors.toSet()))
                && response.spec()
                        .storage()
                        .kind()
                        .equalsIgnoreCase(tableflowTopic.getSpec().getStorage().getKind());
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
