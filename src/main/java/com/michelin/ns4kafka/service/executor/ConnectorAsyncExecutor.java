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

import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorSpecs;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
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

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param connectorRepository The connector repository
     * @param kafkaConnectClient The Kafka Connect client
     */
    public ConnectorAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            ConnectorRepository connectorRepository,
            KafkaConnectClient kafkaConnectClient) {
        this.managedClusterProperties = managedClusterProperties;
        this.connectorRepository = connectorRepository;
        this.kafkaConnectClient = kafkaConnectClient;
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

            return Flux.fromIterable(connectorRepository.findAllForCluster(managedClusterProperties.getName()))
                    .filter(connector -> Connector.DeployStatus.TO_DEPLOY.equals(connector.getDeployStatus()))
                    .flatMap(this::deployConnector);
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
                    connector.setDeployStatus(Connector.DeployStatus.DEPLOYED);
                    connectorRepository.create(connector);

                    log.info(
                            "Success deploying connector {} on Kafka Connect {} of Kafka cluster {}.",
                            connector.getMetadata().getName(),
                            connector.getSpec().getConnectCluster(),
                            managedClusterProperties.getName());
                })
                .doOnError(httpError -> log.error(
                        "Error deploying connector {} on Kafka Connect {} of Kafka cluster {}.",
                        connector.getMetadata().getName(),
                        connector.getSpec().getConnectCluster(),
                        managedClusterProperties.getName(),
                        httpError));
    }
}
