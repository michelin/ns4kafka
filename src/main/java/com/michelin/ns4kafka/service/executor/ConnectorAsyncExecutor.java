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

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import com.michelin.ns4kafka.service.ConnectClusterService;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorSpecs;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorStatus;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Connector executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
@AllArgsConstructor
public class ConnectorAsyncExecutor {
    private final Set<String> healthyConnectClusters = new HashSet<>();
    private final Set<String> idleConnectClusters = new HashSet<>();

    private final ManagedClusterProperties managedClusterProperties;

    private ConnectorRepository connectorRepository;

    private KafkaConnectClient kafkaConnectClient;

    private ConnectClusterService connectClusterService;

    /**
     * Run the connector synchronization.
     *
     * @return A flux of connector info
     */
    public Flux<ConnectorInfo> run() {
        if (managedClusterProperties.isManageConnectors()) {
            return synchronizeConnectors();
        }
        return Flux.empty();
    }

    /**
     * Run the health check.
     *
     * @return A flux of connect cluster
     */
    public Flux<ConnectCluster> runHealthCheck() {
        if (managedClusterProperties.isManageConnectors()) {
            return checkConnectClusterHealth();
        }
        return Flux.empty();
    }

    /**
     * Get all connect clusters of the current Kafka cluster execution, including both self-declared Connect clusters
     * and hard-declared Connect clusters.
     *
     * @return A list of Connect clusters
     */
    private Flux<ConnectCluster> getConnectClusters() {
        return connectClusterService
                .findAll(true)
                .filter(connectCluster ->
                        connectCluster.getMetadata().getCluster().equals(managedClusterProperties.getName()));
    }

    /**
     * Check connect cluster health.
     *
     * @return The list of healthy connect cluster
     */
    private Flux<ConnectCluster> checkConnectClusterHealth() {
        return getConnectClusters().doOnNext(connectCluster -> {
            if (connectCluster.getSpec().getStatus().equals(ConnectCluster.Status.HEALTHY)) {
                log.debug(
                        "Kafka Connect \"{}\" is healthy.",
                        connectCluster.getMetadata().getName());
                healthyConnectClusters.add(connectCluster.getMetadata().getName());
                idleConnectClusters.remove(connectCluster.getMetadata().getName());
            } else if (connectCluster.getSpec().getStatus().equals(ConnectCluster.Status.IDLE)) {
                log.debug(
                        "Kafka Connect \"{}\" is not healthy: {}.",
                        connectCluster.getMetadata().getName(),
                        connectCluster.getSpec().getStatusMessage());
                idleConnectClusters.add(connectCluster.getMetadata().getName());
                healthyConnectClusters.remove(connectCluster.getMetadata().getName());
            }
        });
    }

    /** For each connect cluster, start the synchronization of connectors. */
    private Flux<ConnectorInfo> synchronizeConnectors() {
        log.debug(
                "Starting connector synchronization for Kafka cluster {}. Healthy Kafka Connects: {}."
                        + " Idle Kafka Connects: {}",
                managedClusterProperties.getName(),
                !healthyConnectClusters.isEmpty() ? String.join(",", healthyConnectClusters) : "N/A",
                !idleConnectClusters.isEmpty() ? String.join(",", idleConnectClusters) : "N/A");

        if (healthyConnectClusters.isEmpty()) {
            log.debug(
                    "No healthy Kafka Connect for Kafka cluster {}. Skipping synchronization.",
                    managedClusterProperties.getName());
            return Flux.empty();
        }

        return Flux.fromIterable(healthyConnectClusters).flatMap(this::synchronizeConnectCluster);
    }

    /**
     * Synchronize connectors of given connect cluster.
     *
     * @param connectCluster The connect cluster
     */
    private Flux<ConnectorInfo> synchronizeConnectCluster(String connectCluster) {
        log.debug(
                "Starting connector collection for Kafka cluster {} and Kafka Connect {}.",
                managedClusterProperties.getName(),
                connectCluster);

        return collectBrokerConnectors(connectCluster)
                .doOnError(error -> {
                    if (error instanceof HttpClientResponseException httpClientResponseException) {
                        log.error(
                                "Invalid HTTP response {} ({}) during connectors synchronization for Kafka cluster {}"
                                        + " and Kafka Connect {}.",
                                httpClientResponseException.getStatus(),
                                httpClientResponseException.getResponse().getStatus(),
                                managedClusterProperties.getName(),
                                connectCluster);
                    } else {
                        log.error(
                                "Error during connectors synchronization for Kafka cluster {} and Kafka Connect {}: {}.",
                                managedClusterProperties.getName(),
                                connectCluster,
                                error.getMessage());
                    }
                })
                .collectList()
                .flatMapMany(brokerConnectors -> {
                    List<Connector> ns4kafkaConnectors = collectNs4KafkaConnectors(connectCluster);

                    List<Connector> toCreate = ns4kafkaConnectors.stream()
                            .filter(connector -> brokerConnectors.stream().noneMatch(connector1 -> connector1
                                    .getMetadata()
                                    .getName()
                                    .equals(connector.getMetadata().getName())))
                            .toList();

                    List<Connector> toUpdate = ns4kafkaConnectors.stream()
                            .filter(connector -> brokerConnectors.stream().anyMatch(connector1 -> {
                                if (connector1
                                        .getMetadata()
                                        .getName()
                                        .equals(connector.getMetadata().getName())) {
                                    return !connectorsAreSame(connector, connector1);
                                }
                                return false;
                            }))
                            .toList();

                    if (!toCreate.isEmpty()) {
                        log.debug(
                                "Connector(s) to create: {}",
                                String.join(
                                        ",",
                                        toCreate.stream()
                                                .map(connector ->
                                                        connector.getMetadata().getName())
                                                .toList()));
                    }

                    if (!toUpdate.isEmpty()) {
                        log.debug(
                                "Connector(s) to update: {}",
                                String.join(
                                        ",",
                                        toUpdate.stream()
                                                .map(connector ->
                                                        connector.getMetadata().getName())
                                                .toList()));
                    }

                    return Flux.fromStream(Stream.concat(toCreate.stream(), toUpdate.stream()))
                            .flatMap(this::deployConnector);
                });
    }

    /**
     * Collect the connectors deployed on the given connect cluster.
     *
     * @param connectCluster The connect cluster
     * @return A list of connectors
     */
    public Flux<Connector> collectBrokerConnectors(String connectCluster) {
        return kafkaConnectClient
                .listAll(managedClusterProperties.getName(), connectCluster)
                .flatMapMany(connectors -> {
                    log.debug(
                            "{} connectors found on Kafka Connect {} of Kafka cluster {}.",
                            connectors.size(),
                            connectCluster,
                            managedClusterProperties.getName());

                    return Flux.fromIterable(connectors.values())
                            .map(connectorStatus -> buildConnectorFromConnectorStatus(connectorStatus, connectCluster));
                });
    }

    /**
     * Build a connector from a given connector status.
     *
     * @param connectorStatus The connector status
     * @param connectCluster The connect cluster
     * @return The built connector
     */
    private Connector buildConnectorFromConnectorStatus(ConnectorStatus connectorStatus, String connectCluster) {
        return Connector.builder()
                .metadata(Metadata.builder()
                        // Any other metadata is not useful for this process
                        .name(connectorStatus.info().name())
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster(connectCluster)
                        .config(connectorStatus.info().config())
                        .build())
                .build();
    }

    /**
     * Collect the connectors from Ns4Kafka deployed on the given connect cluster.
     *
     * @param connectCluster The connect cluster
     * @return A list of connectors
     */
    private List<Connector> collectNs4KafkaConnectors(String connectCluster) {
        List<Connector> connectorList =
                connectorRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                        .filter(connector ->
                                connector.getSpec().getConnectCluster().equals(connectCluster))
                        .toList();
        log.debug(
                "{} connectors found in Ns4kafka for Kafka Connect {} of Kafka cluster {}.",
                connectorList.size(),
                connectCluster,
                managedClusterProperties.getName());
        return connectorList;
    }

    /**
     * Check if both given connectors are equal.
     *
     * @param expected The first connector
     * @param actual The second connector
     * @return true it they are, false otherwise
     */
    private boolean connectorsAreSame(Connector expected, Connector actual) {
        Map<String, String> expectedMap = expected.getSpec().getConfig();
        Map<String, String> actualMap = actual.getSpec().getConfig();

        if (expectedMap.size() != actualMap.size()) {
            return false;
        }

        return expectedMap.entrySet().stream()
                .allMatch(e -> (e.getValue() == null && actualMap.get(e.getKey()) == null)
                        || (e.getValue() != null && e.getValue().equals(actualMap.get(e.getKey()))));
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
                .doOnSuccess(httpResponse -> log.info(
                        "Success deploying connector {} on Kafka Connect {} of Kafka cluster {}.",
                        connector.getMetadata().getName(),
                        connector.getSpec().getConnectCluster(),
                        managedClusterProperties.getName()))
                .doOnError(httpError -> log.error(
                        "Error deploying connector {} on Kafka Connect {} of Kafka cluster {}: {}",
                        connector.getMetadata().getName(),
                        connector.getSpec().getConnectCluster(),
                        managedClusterProperties.getName(),
                        httpError.getMessage()));
    }
}
