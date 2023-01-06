package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.ConnectClusterService;
import com.michelin.ns4kafka.services.connect.ConnectorClientProxy;
import com.michelin.ns4kafka.services.connect.client.ConnectorClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStatus;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.internal.observers.ConsumerSingleObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class ConnectorAsyncExecutor {
    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @Inject
    private ConnectorRepository connectorRepository;

    @Inject
    private ConnectorClient connectorClient;

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
        List<String> selfDeclaredConnectClusterNames = connectClusterService.findAll()
                .stream()
                .map(connectCluster -> connectCluster.getMetadata().getName()).toList();

        Stream.concat(kafkaAsyncExecutorConfig.getConnects().keySet().stream(), selfDeclaredConnectClusterNames.stream())
                        .forEach(this::synchronizeConnectCluster);
    }

    /**
     * Synchronize connectors of given connect cluster
     * @param connectCluster The connect cluster
     */
    private void synchronizeConnectCluster(String connectCluster) {
        log.debug("Starting Connector synchronization for Kafka cluster {} and Connect cluster {}",
                kafkaAsyncExecutorConfig.getName(), connectCluster);

        collectBrokerConnectors(connectCluster)
                .subscribe(new ConsumerSingleObserver<>(brokerConnectors -> {
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

                    List<Connector> toDelete = brokerConnectors.stream()
                            .filter(connector -> ns4kafkaConnectors.stream().noneMatch(connector1 -> connector1.getMetadata().getName().equals(connector.getMetadata().getName())))
                            .toList();

                    if (log.isDebugEnabled()) {
                        toCreate.forEach(connector -> log.debug("Connector to create: " + connector.getMetadata().getName()));
                        toUpdate.forEach(connector -> log.debug("Connector to update: " + connector.getMetadata().getName()));
                        log.debug("Connectors not in ns4kafka: " + toDelete.size());
                    }

                    toCreate.forEach(this::deployConnector);
                    toUpdate.forEach(this::deployConnector);
                },
                error -> {
                    if (error instanceof HttpClientResponseException) {
                        log.error("Invalid HTTP response {} ({}) during connectors synchronization for Kafka cluster {} and Connect cluster {}",
                                ((HttpClientResponseException) error).getStatus(), ((HttpClientResponseException) error).getResponse().getStatus(),
                                kafkaAsyncExecutorConfig.getName(), connectCluster);
                    } else if (error instanceof ReadTimeoutException) {
                        log.error("Read timeout during connectors synchronization for Kafka cluster {} and Connect cluster {}",
                                kafkaAsyncExecutorConfig.getName(),
                                connectCluster);
                    } else {
                        log.error("Exception during connectors synchronization for Kafka cluster {} and Connect cluster {} : {}",
                                kafkaAsyncExecutorConfig.getName(),
                                connectCluster, error);
                    }
                }));
    }

    /**
     * Collect the connectors deployed on the given connect cluster
     * @param connectCluster The connect cluster
     * @return A list of connectors
     */
    public Single<List<Connector>> collectBrokerConnectors(String connectCluster) {
        return connectorClient.listAll(ConnectorClientProxy.PROXY_SECRET, kafkaAsyncExecutorConfig.getName(), connectCluster)
                .map(connectors -> {
                    log.debug("Connectors found on Connect cluster {} : {}", connectCluster, connectors.size());

                    return connectors
                            .values()
                            .stream()
                            .map(connectorStatus -> buildConnectorFromConnectorStatus(connectorStatus, connectCluster))
                            .collect(Collectors.toList());
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
                        .name(connectorStatus.getInfo().name())
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster(connectCluster)
                        .config(connectorStatus.getInfo().config())
                        .build())
                .build();
    }

    /**
     * Collect the connectors from Ns4kafka deployed on the given connect cluster
     * @param connectCluster The connect cluster
     * @return A list of connectors
     */
    private List<Connector> collectNs4KafkaConnectors(String connectCluster) {
        List<Connector> connectorList = connectorRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(connector -> connector.getSpec().getConnectCluster().equals(connectCluster))
                .collect(Collectors.toList());
        log.debug("Connectors found on Ns4kafka for Connect cluster {}: {}", connectCluster, connectorList.size());
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
        connectorClient.createOrUpdate(ConnectorClientProxy.PROXY_SECRET, kafkaAsyncExecutorConfig.getName(),
                connector.getSpec().getConnectCluster(), connector.getMetadata().getName(),
                        ConnectorSpecs.builder().config(connector.getSpec().getConfig()).build())
                .subscribe(new ConsumerSingleObserver<>(httpResponse -> log.info("Success deploying Connector [{}] on Kafka [{}] Connect [{}]",
                        connector.getMetadata().getName(), kafkaAsyncExecutorConfig.getName(), connector.getSpec().getConnectCluster()),
                        httpError -> log.error(String.format("Error deploying Connector [%s] on Kafka [%s] Connect [%s]",
                                connector.getMetadata().getName(), kafkaAsyncExecutorConfig.getName(), connector.getSpec().getConnectCluster()))));
    }
}
