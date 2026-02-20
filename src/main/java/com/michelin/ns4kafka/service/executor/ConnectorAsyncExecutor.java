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
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Connector executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class ConnectorAsyncExecutor {
    private static final String SENSITIVE_FIELD_MASK = "••••••••••••";
    private final ManagedClusterProperties managedClusterProperties;
    private final ConnectorRepository connectorRepository;
    private final KafkaConnectClient kafkaConnectClient;
    private final ConnectClusterService connectClusterService;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param connectorRepository The connector repository
     * @param kafkaConnectClient The Kafka Connect client
     * @param connectClusterService The connect cluster service
     */
    public ConnectorAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            ConnectorRepository connectorRepository,
            KafkaConnectClient kafkaConnectClient,
            ConnectClusterService connectClusterService) {
        this.managedClusterProperties = managedClusterProperties;
        this.connectorRepository = connectorRepository;
        this.kafkaConnectClient = kafkaConnectClient;
        this.connectClusterService = connectClusterService;
    }

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
        log.atDebug()
                .addArgument(managedClusterProperties::getName)
                .addArgument(
                        () -> !connectClusterService.getHealthyConnectClusters().isEmpty()
                                ? String.join(",", connectClusterService.getHealthyConnectClusters())
                                : "N/A")
                .log("Starting connector synchronization for Kafka cluster {}. Healthy Kafka Connects: {}.");

        if (connectClusterService.getHealthyConnectClusters().isEmpty()) {
            log.atDebug()
                    .addArgument(managedClusterProperties::getName)
                    .log("No healthy Kafka Connect for Kafka cluster {}. Skipping synchronization.");

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
                .collectMap(connector -> connector.getMetadata().getName())
                .flatMapMany(brokerConnectorsMap -> Flux.fromStream(collectNs4KafkaConnectors(connectCluster))
                        .filter(connector -> {
                            if (connector.getStatus() != null
                                    && connector.getStatus().isToDeploy()) {
                                return true;
                            }

                            Connector clusterConnector = brokerConnectorsMap.get(
                                    connector.getMetadata().getName());
                            return clusterConnector == null || !connectorsAreSame(connector, clusterConnector);
                        })
                        .flatMap(this::deployConnector));
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
    private Stream<Connector> collectNs4KafkaConnectors(String connectCluster) {
        return connectorRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                .filter(connector -> connector.getSpec().getConnectCluster().equals(connectCluster));
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

        return actualMap.entrySet().stream().allMatch(e -> {
            String actualValue = e.getValue();
            String expectedValue = expectedMap.get(e.getKey());
            return (actualValue == null && expectedValue == null)
                    || SENSITIVE_FIELD_MASK.equals(e.getValue())
                    || expectedValue.equals(actualValue);
        });
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
