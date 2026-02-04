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
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
    private static final String SENSITIVE_FIELD_MASK = "••••••••••••";

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
                .findAll(true, true)
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
                connectClusterService
                        .getHealthyConnectClusters()
                        .add(connectCluster.getMetadata().getName());
            } else if (connectCluster.getSpec().getStatus().equals(ConnectCluster.Status.IDLE)) {
                log.debug(
                        "Kafka Connect \"{}\" is not healthy: {}.",
                        connectCluster.getMetadata().getName(),
                        connectCluster.getSpec().getStatusMessage());
                connectClusterService
                        .getHealthyConnectClusters()
                        .remove(connectCluster.getMetadata().getName());
            }
        });
    }

    /** For each connect cluster, start the synchronization of connectors. */
    private Flux<ConnectorInfo> synchronizeConnectors() {
        log.debug(
                "Starting connector synchronization for Kafka cluster {}. Healthy Kafka Connects: {}.",
                managedClusterProperties.getName(),
                !connectClusterService.getHealthyConnectClusters().isEmpty()
                        ? String.join(",", connectClusterService.getHealthyConnectClusters())
                        : "N/A");

        if (connectClusterService.getHealthyConnectClusters().isEmpty()) {
            log.debug(
                    "No healthy Kafka Connect for Kafka cluster {}. Skipping synchronization.",
                    managedClusterProperties.getName());
            return Flux.empty();
        }

        return Flux.fromIterable(connectClusterService.getHealthyConnectClusters())
                .flatMap(this::synchronizeConnectCluster);
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
                    if (error instanceof ResourceValidationException) {
                        connectClusterService.getHealthyConnectClusters().remove(connectCluster);
                    } else if (error instanceof HttpClientResponseException httpClientResponseException) {
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

                    Set<Connector> toDeploy = ns4kafkaConnectors.stream()
                            .filter(connector -> connector.getStatus() != null
                                    && connector.getStatus().isToDeploy())
                            .collect(Collectors.toSet());

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

                    if (!toDeploy.isEmpty()) {
                        log.debug(
                                "Connector(s) to deploy: {}",
                                String.join(
                                        ",",
                                        toDeploy.stream()
                                                .map(connector ->
                                                        connector.getMetadata().getName())
                                                .toList()));
                    }

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

                    toDeploy.addAll(toCreate);
                    toDeploy.addAll(toUpdate);

                    return Flux.fromStream(toDeploy.stream()).flatMap(this::deployConnector);
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
    boolean connectorsAreSame(Connector expected, Connector actual) {
        Map<String, String> expectedMap = expected.getSpec().getConfig();
        Map<String, String> actualMap = actual.getSpec().getConfig();

        if (expectedMap.size() != actualMap.size()) {
            return false;
        }

        return actualMap.entrySet().stream()
                .allMatch(e -> (e.getValue() == null && expectedMap.get(e.getKey()) == null)
                        // Password fields are masked when returned by Connect API in cp 7.9.3+
                        // so these fields mess up the comparison
                        // 2 solutions:
                        // 1) Check with Connect API which field is masked and ignore the
                        // comparison for the concerned fields
                        // 2) Store the mask string and ignore the comparison when the "actual"
                        // value corresponds to this mask
                        // Solution 2 is chosen since it's simpler and has no additional API calls
                        || (Arrays.asList(expectedMap.get(e.getKey()), SENSITIVE_FIELD_MASK)
                                .contains(e.getValue())));
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
                    connector.getStatus().setToDeploy(false);
                    connectorRepository.create(connector);

                    log.info(
                            "Success deploying connector {} on Kafka Connect {} of Kafka cluster {}.",
                            connector.getMetadata().getName(),
                            connector.getSpec().getConnectCluster(),
                            managedClusterProperties.getName());
                })
                .doOnError(httpError -> log.error(
                        "Error deploying connector {} on Kafka Connect {} of Kafka cluster {}: {}",
                        connector.getMetadata().getName(),
                        connector.getSpec().getConnectCluster(),
                        managedClusterProperties.getName(),
                        httpError.getMessage()));
    }
}
