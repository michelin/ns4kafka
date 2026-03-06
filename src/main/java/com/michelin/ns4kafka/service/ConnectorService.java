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
package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConnectorConnectCluster;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConnectorEmptyConnectorClass;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConnectorNoPlugin;
import static com.michelin.ns4kafka.util.config.ConnectorConfig.CONNECTOR_CLASS;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.connect.Connector;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorSpecs;
import com.michelin.ns4kafka.util.FormatErrorUtils;
import com.michelin.ns4kafka.util.RegexUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import jakarta.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Service to manage connectors. */
@Slf4j
@Singleton
public class ConnectorService {
    private final AclService aclService;
    private final ConnectClusterService connectClusterService;
    private final ConnectorRepository connectorRepository;
    private final KafkaConnectClient kafkaConnectClient;

    /**
     * Constructor.
     *
     * @param aclService The ACL service
     * @param connectClusterService The Connect Cluster service
     * @param connectorRepository The connector repository
     * @param kafkaConnectClient The Kafka Connect client
     */
    public ConnectorService(
            AclService aclService,
            ConnectClusterService connectClusterService,
            ConnectorRepository connectorRepository,
            KafkaConnectClient kafkaConnectClient) {
        this.aclService = aclService;
        this.connectClusterService = connectClusterService;
        this.kafkaConnectClient = kafkaConnectClient;
        this.connectorRepository = connectorRepository;
    }

    /**
     * Find all connectors by given namespace.
     *
     * @param namespace The namespace
     * @return A list of connectors
     */
    public List<Connector> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls =
                aclService.findResourceOwnerGrantedToNamespace(namespace, AccessControlEntry.ResourceType.CONNECT);
        return connectorRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
                .filter(connector -> aclService.isResourceCoveredByAcls(
                        acls, connector.getMetadata().getName()))
                .toList();
    }

    /**
     * Find all connectors by given namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @return A list of connectors
     */
    public List<Connector> findByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAllForNamespace(namespace).stream()
                .filter(connector -> RegexUtils.isResourceCoveredByRegex(
                        connector.getMetadata().getName(), nameFilterPatterns))
                .toList();
    }

    /**
     * Find all connectors by given namespace and Connect cluster.
     *
     * @param namespace The namespace
     * @param connectCluster The Connect cluster
     * @return A list of connectors
     */
    public List<Connector> findAllByConnectCluster(Namespace namespace, String connectCluster) {
        return connectorRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
                .filter(connector -> connector.getSpec().getConnectCluster().equals(connectCluster))
                .toList();
    }

    /**
     * Find a connector by namespace and name.
     *
     * @param namespace The namespace
     * @param connector The connector name
     * @return An optional connector
     */
    public Optional<Connector> findByName(Namespace namespace, String connector) {
        return findAllForNamespace(namespace).stream()
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .findFirst();
    }

    /**
     * Validate configurations of a given connector against the namespace rules.
     *
     * @param namespace The namespace
     * @param connector The connector to validate
     * @return A list of errors
     */
    public Mono<List<String>> validateLocally(Namespace namespace, Connector connector) {
        // Check whether target Connect Cluster is allowed for this namespace
        List<String> selfDeployedConnectClusters =
                connectClusterService.findAllForNamespaceWithWritePermission(namespace).stream()
                        .map(connectCluster -> connectCluster.getMetadata().getName())
                        .toList();

        if (!namespace
                        .getSpec()
                        .getConnectClusters()
                        .contains(connector.getSpec().getConnectCluster())
                && !selfDeployedConnectClusters.contains(connector.getSpec().getConnectCluster())) {
            String allowedConnectClusters = Stream.concat(
                            namespace.getSpec().getConnectClusters().stream(), selfDeployedConnectClusters.stream())
                    .collect(Collectors.joining(", "));
            return Mono.just(List.of(
                    invalidConnectorConnectCluster(connector.getSpec().getConnectCluster(), allowedConnectClusters)));
        }

        // If class does not exist, no need to go further
        if (StringUtils.isEmpty(connector.getSpec().getConfig().get(CONNECTOR_CLASS))) {
            return Mono.just(List.of(invalidConnectorEmptyConnectorClass()));
        }

        // Connector type exists on this target connect cluster
        return kafkaConnectClient
                .connectPlugins(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster())
                .map(connectorPluginInfos -> {
                    Optional<String> connectorType = connectorPluginInfos.stream()
                            .filter(connectPluginItem -> connectPluginItem
                                    .className()
                                    .equals(connector.getSpec().getConfig().get(CONNECTOR_CLASS)))
                            .map(connectorPluginInfo ->
                                    connectorPluginInfo.type().toString().toLowerCase(Locale.ROOT))
                            .findFirst();

                    if (connectorType.isEmpty()) {
                        return List.of(invalidConnectorNoPlugin(
                                connector.getSpec().getConfig().get(CONNECTOR_CLASS)));
                    }

                    return namespace.getSpec().getConnectValidator() != null
                            ? namespace.getSpec().getConnectValidator().validate(connector, connectorType.get())
                            : Collections.emptyList();
                });
    }

    /**
     * Is given namespace owner of the given connector.
     *
     * @param namespace The namespace
     * @param connect The connector
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfConnect(Namespace namespace, String connect) {
        return aclService.isNamespaceOwnerOfResource(
                namespace.getMetadata().getName(), AccessControlEntry.ResourceType.CONNECT, connect);
    }

    /**
     * Validate configurations of a given connector against the cluster.
     *
     * @param namespace The namespace
     * @param connector The connector
     * @return A list of errors
     */
    public Mono<List<String>> validateRemotely(Namespace namespace, Connector connector) {
        return kafkaConnectClient
                .validate(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getSpec().getConfig().get(CONNECTOR_CLASS),
                        ConnectorSpecs.builder()
                                .config(connector.getSpec().getConfig())
                                .build())
                .map(configInfos -> configInfos.configs().stream()
                        .filter(configInfo -> !configInfo.configValue().errors().isEmpty())
                        .flatMap(configInfo -> configInfo.configValue().errors().stream()
                                .map(error -> FormatErrorUtils.invalidConnectorRemote(
                                        connector.getMetadata().getName(), error)))
                        .toList());
    }

    /**
     * Create a given connector.
     *
     * @param connector The connector to create
     * @return The created connector
     */
    public Connector createOrUpdate(Connector connector) {
        return connectorRepository.create(connector);
    }

    /**
     * Delete a given connector.
     *
     * @param namespace The namespace
     * @param connector The connector
     */
    public Mono<HttpResponse<Void>> delete(Namespace namespace, Connector connector, boolean force) {
        return kafkaConnectClient
                .delete(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName())
                .defaultIfEmpty(HttpResponse.noContent())
                .onErrorResume(error -> {
                    if (force) {
                        log.atInfo()
                                .addArgument(connector.getMetadata().getName())
                                .addArgument(namespace.getMetadata().getName())
                                .addArgument(connector.getSpec().getConnectCluster())
                                .addArgument(error.getMessage())
                                .log("Success force deleting Connector [{}] on Namespace [{}] from repository."
                                        + " Failed to delete from Connect cluster [{}]: [{}]");
                        return Mono.just(HttpResponse.noContent());
                    }
                    return Mono.error(error);
                })
                .map(httpResponse -> {
                    connectorRepository.delete(connector);

                    log.atInfo()
                            .addArgument(connector.getMetadata().getName())
                            .addArgument(namespace.getMetadata().getName())
                            .addArgument(connector.getSpec().getConnectCluster())
                            .log("Success removing Connector [{}] on Kafka [{}] Connect [{}]");

                    return httpResponse;
                });
    }

    /**
     * List all connectors of a given namespace that are not synchronized to Ns4Kafka, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @return The list of connectors
     */
    public Flux<Connector> listUnsynchronizedConnectorsByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));

        // Get all connectors from all connect clusters
        Stream<String> connectClusters = Stream.concat(
                namespace.getSpec().getConnectClusters().stream(),
                connectClusterService.findAllForNamespaceWithWritePermission(namespace).stream()
                        .map(connectCluster -> connectCluster.getMetadata().getName()));

        return Flux.fromStream(connectClusters).flatMap(connectClusterName -> kafkaConnectClient
                .listAll(namespace.getMetadata().getCluster(), connectClusterName)
                .flatMapMany(
                        connectors -> Flux.fromIterable(connectors.values()).map(connectorStatus -> Connector.builder()
                                .metadata(Metadata.builder()
                                        .name(connectorStatus.info().name())
                                        .build())
                                .spec(Connector.ConnectorSpec.builder()
                                        .connectCluster(connectClusterName)
                                        .config(connectorStatus.info().config())
                                        .build())
                                .build()))
                .filter(connector ->
                        // ...that belongs to this namespace
                        isNamespaceOwnerOfConnect(
                                        namespace, connector.getMetadata().getName())
                                // ...and aren't in Ns4Kafka storage
                                && findByName(namespace, connector.getMetadata().getName())
                                        .isEmpty()
                                // ...and match the name parameter
                                && RegexUtils.isResourceCoveredByRegex(
                                        connector.getMetadata().getName(), nameFilterPatterns)));
    }

    /**
     * Restart a given connector.
     *
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Mono<HttpResponse<Void>> restart(Namespace namespace, Connector connector) {
        return kafkaConnectClient
                .status(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName())
                .flatMap(status -> Flux.fromIterable(status.tasks())
                        .flatMap(task -> kafkaConnectClient.restart(
                                namespace.getMetadata().getCluster(),
                                connector.getSpec().getConnectCluster(),
                                connector.getMetadata().getName(),
                                task.getId()))
                        .doOnNext(_ -> log.info(
                                "Success restarting connector {} on namespace {} Kafka Connect {}.",
                                connector.getMetadata().getName(),
                                namespace.getMetadata().getName(),
                                connector.getSpec().getConnectCluster()))
                        .then(Mono.just(HttpResponse.ok())));
    }

    /**
     * Pause a given connector.
     *
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Mono<HttpResponse<Void>> pause(Namespace namespace, Connector connector) {
        return kafkaConnectClient
                .pause(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName())
                .map(_ -> {
                    log.info(
                            "Success pausing connector {} on namespace {} and Kafka Connect {}.",
                            connector.getMetadata().getName(),
                            namespace.getMetadata().getName(),
                            connector.getSpec().getConnectCluster());

                    return HttpResponse.accepted();
                });
    }

    /**
     * Resume a given connector.
     *
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Mono<HttpResponse<Void>> resume(Namespace namespace, Connector connector) {
        return kafkaConnectClient
                .resume(
                        namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName())
                .map(_ -> {
                    log.info(
                            "Success resuming connector {} on namespace {} and Kafka Connect {}.",
                            connector.getMetadata().getName(),
                            namespace.getMetadata().getName(),
                            connector.getSpec().getConnectCluster());

                    return HttpResponse.accepted();
                });
    }
}
