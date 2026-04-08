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

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.connect.Connector;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import com.michelin.ns4kafka.service.ConnectorService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorSpecs;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.HttpResponse;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Connector executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class ConnectorAsyncExecutor {
    private final ManagedClusterProperties managedClusterProperties;
    private final ConnectorRepository connectorRepository;
    private final KafkaConnectClient kafkaConnectClient;
    private final ConnectorService connectorService;
    private final NamespaceService namespaceService;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param connectorRepository The connector repository
     * @param kafkaConnectClient The Kafka Connect client
     * @param connectorService The connector service
     * @param namespaceService The namespace service
     */
    public ConnectorAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            ConnectorRepository connectorRepository,
            KafkaConnectClient kafkaConnectClient,
            ConnectorService connectorService,
            NamespaceService namespaceService) {
        this.managedClusterProperties = managedClusterProperties;
        this.connectorRepository = connectorRepository;
        this.kafkaConnectClient = kafkaConnectClient;
        this.connectorService = connectorService;
        this.namespaceService = namespaceService;
    }

    /**
     * Run the connector synchronization.
     *
     * @return A flux of connector info
     */
    public Flux<ConnectorInfo> run() {
        if (managedClusterProperties.isManageConnectors()) {
            log.atDebug()
                    .addArgument(managedClusterProperties::getName)
                    .log("Starting connector synchronization for Kafka cluster {}.");

            List<Connector> allConnectors = connectorRepository.findAllForCluster(managedClusterProperties.getName());

            Flux<ConnectorInfo> deployFlux =
                    Flux.fromIterable(allConnectors).filter(Resource::isPending).flatMap(this::deployConnector);

            Flux<ConnectorInfo> deleteFlux = Flux.fromIterable(allConnectors)
                    .filter(Resource::isDeleting)
                    .flatMap(this::deleteConnector);

            return Flux.merge(deployFlux, deleteFlux);
        }

        return Flux.empty();
    }

    /**
     * Deploy a given connector to associated connect cluster.
     *
     * @param connector The connector to deploy
     */
    private Mono<ConnectorInfo> deployConnector(Connector connector) {
        return kafkaConnectClient
                .createOrUpdate(
                        managedClusterProperties.getName(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName(),
                        ConnectorSpecs.builder()
                                .config(connector.getSpec().getConfig())
                                .build())
                .doOnSuccess(_ -> {
                    // Do not mark connector as success if it has been marked has pending by another update
                    if (isUnchangedSinceLastApply(connector)) {
                        connector
                                .getMetadata()
                                .setGeneration(connector.getMetadata().getGeneration() + 1);
                        connector.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                        connectorRepository.create(connector);

                        log.info(
                                "Success deploying connector {} on Kafka Connect {} of Kafka cluster {}.",
                                connector.getMetadata().getName(),
                                connector.getSpec().getConnectCluster(),
                                managedClusterProperties.getName());
                    }
                })
                .doOnError(httpError -> {
                    // Do not mark connector as failed if it has been marked has pending by another update
                    if (isUnchangedSinceLastApply(connector)) {
                        connector.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(httpError.getMessage()));
                        connectorRepository.create(connector);

                        log.error(
                                "Error deploying connector {} on Kafka Connect {} of Kafka cluster {}: {}.",
                                connector.getMetadata().getName(),
                                connector.getSpec().getConnectCluster(),
                                managedClusterProperties.getName(),
                                httpError.getMessage());
                    }
                });
    }

    /**
     * Delete a given connector from the associated connect cluster.
     *
     * @param connector The connector to delete
     */
    private Mono<ConnectorInfo> deleteConnector(Connector connector) {
        boolean force = Boolean.parseBoolean(
                connector.getMetadata().getStatus().getOptions() != null
                        ? connector.getMetadata().getStatus().getOptions().getOrDefault("force", "false")
                        : "false");

        Optional<Namespace> existingNamespace =
                namespaceService.findByName(connector.getMetadata().getNamespace());

        if (existingNamespace.isEmpty()) {
            log.error(
                    "Error deleting connector {}: namespace {} not found.",
                    connector.getMetadata().getName(),
                    connector.getMetadata().getNamespace());
            return Mono.empty();
        }

        return kafkaConnectClient
                .delete(
                        existingNamespace.get().getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName())
                .defaultIfEmpty(HttpResponse.noContent())
                .onErrorResume(error -> force ? Mono.just(HttpResponse.noContent()) : Mono.error(error))
                .doOnNext(_ -> {
                    // Do not delete connector if it has been marked has pending by another update
                    if (isUnchangedSinceLastApply(connector)) {
                        connectorRepository.delete(connector);

                        log.info(
                                "Success deleting connector {} on Kafka Connect {} of Kafka cluster {}.",
                                connector.getMetadata().getName(),
                                connector.getSpec().getConnectCluster(),
                                managedClusterProperties.getName());
                    }
                })
                .doOnError(httpError -> {
                    // Do not mark connector as failed if it has been marked has pending by another update
                    if (isUnchangedSinceLastApply(connector)) {
                        connector.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(httpError.getMessage()));
                        connectorRepository.create(connector);

                        log.error(
                                "Error deleting connector {} on Kafka Connect {} of Kafka cluster {}: {}.",
                                connector.getMetadata().getName(),
                                connector.getSpec().getConnectCluster(),
                                managedClusterProperties.getName(),
                                httpError.getMessage());
                    }
                })
                .then(Mono.empty());
    }

    /**
     * Checks whether the connector has been reapplied since the last deployment. Avoids publishing over a connector
     * that has already been changed.
     *
     * @param connector The connector to deploy
     * @return True if it has been reapplied, false otherwise
     */
    private boolean isUnchangedSinceLastApply(Connector connector) {
        Optional<Namespace> existingNamespace =
                namespaceService.findByName(connector.getMetadata().getNamespace());
        if (existingNamespace.isPresent()) {
            Optional<Connector> existingConnector = connectorService.findByName(
                    existingNamespace.get(), connector.getMetadata().getName());
            return existingConnector.isEmpty()
                    || !existingConnector
                            .get()
                            .getMetadata()
                            .getCreationTimestamp()
                            .after(connector.getMetadata().getCreationTimestamp());
        }

        return true;
    }
}
