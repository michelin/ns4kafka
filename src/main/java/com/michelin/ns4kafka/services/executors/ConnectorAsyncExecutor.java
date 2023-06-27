package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.ConnectClusterService;
import com.michelin.ns4kafka.services.clients.connect.KafkaConnectClient;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorStatus;
import com.michelin.ns4kafka.services.clients.connect.entities.ServerInfo;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
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

    public ConnectorAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    /**
     * Start connector synchronization
     */
    public void run() {
        if (kafkaAsyncExecutorConfig.isManageConnectors()) {
            synchronizeConnectors();
        }
    }

    /**
     * For each connect cluster, start the synchronization of connectors
     */
    private void synchronizeConnectors() {
        log.debug("Starting Connect cluster collection for cluster {}", kafkaAsyncExecutorConfig.getName());

        List<String> selfDeclaredConnectClusterNames = connectClusterService.findAll()
                .stream()
                .filter(connectCluster -> connectCluster.getMetadata().getCluster().equals(kafkaAsyncExecutorConfig.getName()))
                .map(connectCluster -> connectCluster.getMetadata().getName()).toList();

        Stream.concat(kafkaAsyncExecutorConfig.getConnects().keySet().stream(), selfDeclaredConnectClusterNames.stream())
                .filter(connectCluster -> {
                    try {
                        HttpResponse<ServerInfo> response = kafkaConnectClient.version(kafkaAsyncExecutorConfig.getName(), connectCluster);
                        return response.status().equals(HttpStatus.OK);
                    } catch (Exception e) {
                        return false;
                    }
                })
                .forEach(this::synchronizeConnectCluster);
    }

    /**
     * Synchronize connectors of given connect cluster
     * @param connectCluster The connect cluster
     */
    private void synchronizeConnectCluster(String connectCluster) {
        log.debug("Starting connector collection for Kafka cluster {} and Connect cluster {}",
                kafkaAsyncExecutorConfig.getName(), connectCluster);

        collectBrokerConnectors(connectCluster)
                .subscribe(brokerConnectors -> {
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

                    if (log.isDebugEnabled()) {
                        toCreate.forEach(connector -> log.debug("Connector to create: " + connector.getMetadata().getName()));
                        toUpdate.forEach(connector -> log.debug("Connector to update: " + connector.getMetadata().getName()));
                    }

                    toCreate.forEach(this::deployConnector);
                    toUpdate.forEach(this::deployConnector);
                },
                error -> {
                    if (error instanceof HttpClientResponseException httpClientResponseException) {
                        log.error("Invalid HTTP response {} ({}) during connectors synchronization for Kafka cluster {} and Kafka Connect {}",
                                httpClientResponseException.getStatus(), httpClientResponseException.getResponse().getStatus(),
                                kafkaAsyncExecutorConfig.getName(), connectCluster);
                    } else if (error instanceof ReadTimeoutException) {
                        log.error("Read timeout during connectors synchronization for Kafka cluster {} and Kafka Connect {}",
                                kafkaAsyncExecutorConfig.getName(), connectCluster);
                    } else {
                        log.error("Exception during connectors synchronization for Kafka cluster {} and Kafka Connect {}",
                                kafkaAsyncExecutorConfig.getName(), connectCluster, error);
                    }
                });
    }

    /**
     * Collect the connectors deployed on the given connect cluster
     * @param connectCluster The connect cluster
     * @return A list of connectors
     */
    public Mono<List<Connector>> collectBrokerConnectors(String connectCluster) {
        return kafkaConnectClient.listAll(kafkaAsyncExecutorConfig.getName(), connectCluster)
                .map(connectors -> {
                    log.debug("Connectors found on Connect cluster {} of Kafka cluster {}: {}", connectCluster, kafkaAsyncExecutorConfig.getName(), connectors.size());

                    return connectors
                            .values()
                            .stream()
                            .map(connectorStatus -> buildConnectorFromConnectorStatus(connectorStatus, connectCluster))
                            .toList();
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
        log.debug("Connectors found in Ns4kafka for Connect cluster {} of Kafka cluster {}: {}", connectCluster, kafkaAsyncExecutorConfig.getName(), connectorList.size());
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
    private void deployConnector(Connector connector) {
        kafkaConnectClient.createOrUpdate(kafkaAsyncExecutorConfig.getName(), connector.getSpec().getConnectCluster(),
                        connector.getMetadata().getName(), ConnectorSpecs.builder().config(connector.getSpec().getConfig()).build())
                .subscribe(httpResponse -> log.info("Success deploying Connector [{}] on Kafka [{}] Connect [{}]",
                        connector.getMetadata().getName(), kafkaAsyncExecutorConfig.getName(), connector.getSpec().getConnectCluster()),
                        httpError -> log.error(String.format("Error deploying Connector [%s] on Kafka [%s] Connect [%s]",
                                connector.getMetadata().getName(), kafkaAsyncExecutorConfig.getName(), connector.getSpec().getConnectCluster())));
    }
}
