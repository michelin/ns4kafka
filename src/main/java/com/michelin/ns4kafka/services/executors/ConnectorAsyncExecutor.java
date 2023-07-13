package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.ConnectClusterService;
import com.michelin.ns4kafka.services.clients.connect.KafkaConnectClient;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorStatus;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class ConnectorAsyncExecutor {
    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @Inject
    private ConnectorRepository connectorRepository;

    @Inject
    private KafkaConnectClient kafkaConnectClient;

    @Inject
    private ConnectClusterService connectClusterService;

    private final Set<String> healthyConnectClusters = new HashSet<>();
    private final Set<String> idleConnectClusters = new HashSet<>();

    public ConnectorAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    /**
     * Start connector synchronization
     */
    public Flux<ConnectorInfo> run() {
        if (kafkaAsyncExecutorConfig.isManageConnectors()) {
            return synchronizeConnectors();
        }
        return Flux.empty();
    }

    /**
     * Start connector synchronization
     */
    public Flux<ConnectCluster> runHealthCheck() {
        if (kafkaAsyncExecutorConfig.isManageConnectors()) {
            return checkConnectClusterHealth();
        }
        return Flux.empty();
    }

    /**
     * Get all connect clusters of the current Kafka cluster execution, including
     * both self-declared Connect clusters and hard-declared Connect clusters
     * @return A list of Connect clusters
     */
    private Flux<ConnectCluster> getConnectClusters() {
        return connectClusterService.findAll(true)
                .filter(connectCluster -> connectCluster.getMetadata().getCluster().equals(kafkaAsyncExecutorConfig.getName()));
    }

    /**
     * Check connect cluster health
     * @return The list of healthy connect cluster
     */
    private Flux<ConnectCluster> checkConnectClusterHealth() {
        return getConnectClusters()
                .doOnNext(connectCluster -> {
                    if (connectCluster.getSpec().getStatus().equals(ConnectCluster.Status.HEALTHY)) {
                        log.debug("Kafka Connect \"" + connectCluster.getMetadata().getName() + "\" is healthy.");
                        healthyConnectClusters.add(connectCluster.getMetadata().getName());
                        idleConnectClusters.remove(connectCluster.getMetadata().getName());
                    } else if (connectCluster.getSpec().getStatus().equals(ConnectCluster.Status.IDLE)) {
                        log.debug("Kafka Connect \"" + connectCluster.getMetadata().getName() + "\" is not healthy: " + connectCluster.getSpec().getStatusMessage() + ".");
                        idleConnectClusters.add(connectCluster.getMetadata().getName());
                        healthyConnectClusters.remove(connectCluster.getMetadata().getName());
                    }
                });
    }


    /**
     * For each connect cluster, start the synchronization of connectors
     */
    private Flux<ConnectorInfo> synchronizeConnectors() {
        log.debug("Starting connector synchronization for Kafka cluster {}. Healthy Kafka Connects: {}. Idle Kafka Connects: {}",
                kafkaAsyncExecutorConfig.getName(),
                !healthyConnectClusters.isEmpty() ? String.join(",", healthyConnectClusters) : "N/A",
                !idleConnectClusters.isEmpty() ? String.join(",", idleConnectClusters) : "N/A");

        if (healthyConnectClusters.isEmpty()) {
            log.debug("No healthy Kafka Connect for Kafka cluster {}. Skipping synchronization.", kafkaAsyncExecutorConfig.getName());
            return Flux.empty();
        }

        return Flux.fromIterable(healthyConnectClusters)
                .flatMap(this::synchronizeConnectCluster);
    }

    /**
     * Synchronize connectors of given connect cluster
     * @param connectCluster The connect cluster
     */
    private Flux<ConnectorInfo> synchronizeConnectCluster(String connectCluster) {
        log.debug("Starting connector collection for Kafka cluster {} and Kafka Connect {}.",
                kafkaAsyncExecutorConfig.getName(), connectCluster);

        return collectBrokerConnectors(connectCluster)
            .doOnError(error -> {
                if (error instanceof HttpClientResponseException httpClientResponseException) {
                    log.error("Invalid HTTP response {} ({}) during connectors synchronization for Kafka cluster {} and Kafka Connect {}.",
                            httpClientResponseException.getStatus(), httpClientResponseException.getResponse().getStatus(),
                            kafkaAsyncExecutorConfig.getName(), connectCluster);
                } else {
                    log.error("Exception during connectors synchronization for Kafka cluster {} and Kafka Connect {}: {}.",
                            kafkaAsyncExecutorConfig.getName(), connectCluster, error.getMessage());
                }
            })
            .collectList()
            .flatMapMany(brokerConnectors -> {
                List<Connector> ns4kafkaConnectors = collectNs4KafkaConnectors(connectCluster);

                List<Connector> toCreate = ns4kafkaConnectors.stream()
                        .filter(connector -> brokerConnectors.stream().noneMatch(connector1 -> connector1.getMetadata().getName().equals(connector.getMetadata().getName())))
                        .toList();

                List<Connector> toUpdate = ns4kafkaConnectors.stream()
                        .filter(connector -> brokerConnectors.stream()
                                .anyMatch(connector1 -> {
                                    if (connector1.getMetadata().getName().equals(connector.getMetadata().getName())) {
                                        return !connectorsAreSame(connector, connector1);
                                    }
                                    return false;
                                }))
                        .toList();

                if (!toCreate.isEmpty()) {
                    log.debug("Connector(s) to create: " + String.join(",", toCreate.stream().map(connector -> connector.getMetadata().getName()).toList()));
                }

                if (!toUpdate.isEmpty()) {
                    log.debug("Connector(s) to update: " + String.join(",", toUpdate.stream().map(connector -> connector.getMetadata().getName()).toList()));
                }

                return Flux.fromStream(Stream.concat(toCreate.stream(), toUpdate.stream()))
                        .flatMap(this::deployConnector);
            });
    }

    /**
     * Collect the connectors deployed on the given connect cluster
     * @param connectCluster The connect cluster
     * @return A list of connectors
     */
    public Flux<Connector> collectBrokerConnectors(String connectCluster) {
        return kafkaConnectClient.listAll(kafkaAsyncExecutorConfig.getName(), connectCluster)
                .flatMapMany(connectors -> {
                    log.debug("{} connectors found on Kafka Connect {} of Kafka cluster {}.", connectors.size(), connectCluster, kafkaAsyncExecutorConfig.getName());

                    return Flux.fromIterable(connectors.values())
                            .map(connectorStatus -> buildConnectorFromConnectorStatus(connectorStatus, connectCluster));
                });
    }

    /**
     * Build a connector from a given connector status
     * @param connectorStatus The connector status
     * @param connectCluster The connect cluster
     * @return The built connector
     */
    private Connector buildConnectorFromConnectorStatus(ConnectorStatus connectorStatus, String connectCluster) {
        return Connector.builder()
                .metadata(ObjectMeta.builder()
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
     * Collect the connectors from Ns4Kafka deployed on the given connect cluster
     * @param connectCluster The connect cluster
     * @return A list of connectors
     */
    private List<Connector> collectNs4KafkaConnectors(String connectCluster) {
        List<Connector> connectorList = connectorRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(connector -> connector.getSpec().getConnectCluster().equals(connectCluster))
                .toList();
        log.debug("{} connectors found in Ns4kafka for Kafka Connect {} of Kafka cluster {}.", connectorList.size(), connectCluster, kafkaAsyncExecutorConfig.getName());
        return connectorList;
    }

    /**
     * Check if both given connectors are equal
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

        return expectedMap.entrySet()
                .stream()
                .allMatch(e -> (e.getValue() == null && actualMap.get(e.getKey()) == null)
                        || (e.getValue() != null && e.getValue().equals(actualMap.get(e.getKey()))));
    }

    /**
     * Deploy a given connector to associated connect cluster
     * @param connector The connector to deploy
     */
    private Mono<ConnectorInfo> deployConnector(Connector connector) {
        return kafkaConnectClient.createOrUpdate(kafkaAsyncExecutorConfig.getName(), connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName(), ConnectorSpecs.builder().config(connector.getSpec().getConfig()).build())
                .doOnSuccess(httpResponse -> log.info("Success deploying connector {} on Kafka Connect {} of Kafka cluster {}.",
                        connector.getMetadata().getName(), connector.getSpec().getConnectCluster(), kafkaAsyncExecutorConfig.getName()))
                .doOnError(httpError -> log.error("Error deploying connector {} on Kafka Connect {} of Kafka cluster {}.",
                                connector.getMetadata().getName(), connector.getSpec().getConnectCluster(), kafkaAsyncExecutorConfig.getName()));
    }
}
