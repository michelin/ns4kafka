package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectClientProxy;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStatus;
import io.micronaut.context.annotation.EachBean;
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

    private KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;
    @Inject
    ConnectorRepository connectorRepository;
    @Inject
    KafkaConnectClient kafkaConnectClient;

    public ConnectorAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    public void run() {
        if (kafkaAsyncExecutorConfig.isManageConnectors()) {
            synchronizeConnectors();
        }
    }

    private void synchronizeConnectors() {
        kafkaAsyncExecutorConfig.getConnects()
                .forEach((s, connectConfig) -> synchronizeConnectCluster(s));
    }

    private void synchronizeConnectCluster(String connectCluster) {
        log.debug("Starting Connector synchronization for Kafka cluster {} and Connect cluster {}",
                kafkaAsyncExecutorConfig.getName(),
                connectCluster);
        try {
            // List Connectors from cluster
            List<Connector> brokerConnectors = collectBrokerConnectors(connectCluster);
            // List Connectors from ns4kafka
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
                toCreate.forEach(connector -> log.debug("to create : {}",connector.getMetadata().getName()));
                toUpdate.forEach(connector -> log.debug("to update : {}",connector.getMetadata().getName()));
                log.debug("not in ns4kafka : {}", toDelete.size());
            }

            toCreate.forEach(this::deployConnector);
            toUpdate.forEach(this::deployConnector);


        } catch (Exception e) {
            log.error("Exception during Connectors synchronization", e);
        }
    }

    public List<Connector> collectBrokerConnectors(String connectCluster) {
        List<Connector> connectorList = kafkaConnectClient.listAll(KafkaConnectClientProxy.PROXY_SECRET, kafkaAsyncExecutorConfig.getName(), connectCluster)
                .values()
                .stream()
                .map(connectorStatus -> buildConnectorFromConnectorStatus(connectorStatus, connectCluster))
                .collect(Collectors.toList());
        log.debug("Connectors found on Connect Cluster {} : {}", connectCluster, connectorList.size());
        return connectorList;
    }

    private Connector buildConnectorFromConnectorStatus(ConnectorStatus connectorStatus, String connectCluster) {
        return Connector.builder()
                .metadata(ObjectMeta.builder()
                        // Any other metadata is not usefull for this process
                        .name(connectorStatus.getInfo().name())
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster(connectCluster)
                        .config(connectorStatus.getInfo().config())
                        .build())
                .build();
    }

    private List<Connector> collectNs4KafkaConnectors(String connectCluster) {
        List<Connector> connectorList = connectorRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(connector -> connector.getSpec().getConnectCluster().equals(connectCluster))
                .collect(Collectors.toList());
        log.debug("Connectors found on ns4kafka for Connect Cluster {} : {}", connectCluster, connectorList.size());
        return connectorList;
    }

    private boolean connectorsAreSame(Connector expected, Connector actual) {
        Map<String, String> expectedMap = expected.getSpec().getConfig();
        Map<String, String> actualMap = actual.getSpec().getConfig();
        if (expectedMap.size() != actualMap.size()) {
            return false;
        }
        return expectedMap.entrySet().stream()
                .allMatch(e -> e.getValue().equals(actualMap.get(e.getKey())));
    }

    private void deployConnector(Connector connector) {
        try {
            kafkaConnectClient.createOrUpdate(
                    KafkaConnectClientProxy.PROXY_SECRET,
                    kafkaAsyncExecutorConfig.getName(),
                    connector.getSpec().getConnectCluster(),
                    connector.getMetadata().getName(),
                    connector.getSpec().getConfig());
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
