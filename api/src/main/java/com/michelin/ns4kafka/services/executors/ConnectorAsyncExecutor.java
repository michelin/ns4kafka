package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectClientProxy;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStatus;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class ConnectorAsyncExecutor {
    /**
     * The managed clusters config
     */
    @Inject
    private KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    /**
     * The connector repository
     */
    @Inject
    private ConnectorRepository connectorRepository;

    /**
     * The Kafka Connect client
     */
    @Inject
    private KafkaConnectClient kafkaConnectClient;

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
        kafkaAsyncExecutorConfig.getConnects()
                .forEach((s, connectConfig) -> synchronizeConnectCluster(s));
    }

    /**
     * Synchronize connectors of given connect cluster
     * @param connectCluster The connect cluster
     */
    private void synchronizeConnectCluster(String connectCluster) {
        log.debug("Starting Connector synchronization for Kafka cluster {} and Connect cluster {}",
                kafkaAsyncExecutorConfig.getName(),
                connectCluster);

        try {
            List<Connector> brokerConnectors = collectBrokerConnectors(connectCluster);
            List<Connector> ns4kafkaConnectors = collectNs4KafkaConnectors(connectCluster);

            List<Connector> toCreate = ns4kafkaConnectors.stream()
                    .filter(connector -> brokerConnectors.stream().noneMatch(connector1 -> connector1.getMetadata().getName().equals(connector.getMetadata().getName())))
                    .collect(Collectors.toList());

            List<Connector> toUpdate = ns4kafkaConnectors.stream()
                    .filter(connector -> brokerConnectors.stream()
                            .anyMatch(connector1 -> {
                                if (connector1.getMetadata().getName().equals(connector.getMetadata().getName())) {
                                    return !connectorsAreSame(connector, connector1);
                                }
                                return false;
                            }))
                    .collect(Collectors.toList());

            List<Connector> toDelete = brokerConnectors.stream()
                    .filter(connector -> ns4kafkaConnectors.stream().noneMatch(connector1 -> connector1.getMetadata().getName().equals(connector.getMetadata().getName())))
                    .collect(Collectors.toList());

            if (log.isDebugEnabled()) {
                toCreate.forEach(connector -> log.debug("to create : " + connector.getMetadata().getName()));
                toUpdate.forEach(connector -> log.debug("to update : " + connector.getMetadata().getName()));
                log.debug("not in ns4kafka : " + toDelete.size());
            }

            toCreate.forEach(this::deployConnector);
            toUpdate.forEach(this::deployConnector);
        } catch (HttpClientResponseException e) {
            log.error("Invalid Http response {} during Connectors synchronization for Kafka cluster {} and Connect cluster {}",
                    e.getStatus(),
                    kafkaAsyncExecutorConfig.getName(),
                    connectCluster);
        } catch (ReadTimeoutException e){
            log.error("ReadTimeoutException during Connectors synchronization for Kafka cluster {} and Connect cluster {}",
                    kafkaAsyncExecutorConfig.getName(),
                    connectCluster);
        } catch (Exception e) {
            log.error("Exception during Connectors synchronization for Kafka cluster {} and Connect cluster {} : {}",
                    kafkaAsyncExecutorConfig.getName(),
                    connectCluster, e);
        }
    }

    /**
     * Collect the connectors deployed on the given connect cluster
     * @param connectCluster The connect cluster
     * @return A list of connectors
     */
    public List<Connector> collectBrokerConnectors(String connectCluster) {
        List<Connector> connectorList = kafkaConnectClient.listAll(KafkaConnectClientProxy.PROXY_SECRET, kafkaAsyncExecutorConfig.getName(), connectCluster)
                .values()
                .stream()
                .map(connectorStatus -> buildConnectorFromConnectorStatus(connectorStatus, connectCluster))
                .collect(Collectors.toList());
        log.debug("Connectors found on Connect Cluster {} : {}", connectCluster, connectorList.size());
        return connectorList;
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
        log.debug("Connectors found on ns4kafka for Connect Cluster {} : {}", connectCluster, connectorList.size());
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
        try {
            kafkaConnectClient.createOrUpdate(
                    KafkaConnectClientProxy.PROXY_SECRET,
                    kafkaAsyncExecutorConfig.getName(),
                    connector.getSpec().getConnectCluster(),
                    connector.getMetadata().getName(),
                    ConnectorSpecs.builder()
                            .config(connector.getSpec().getConfig())
                            .build());

            log.info("Success deploying Connector [{}] on Kafka [{}] Connect [{}]",
                    connector.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getName(),
                    connector.getSpec().getConnectCluster());
        } catch (Exception e) {
            log.error(String.format("Error deploying Connector [%s] on Kafka [%s] Connect [%s]",
                    connector.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getName(),
                    connector.getSpec().getConnectCluster()), e);
        }
    }
}
