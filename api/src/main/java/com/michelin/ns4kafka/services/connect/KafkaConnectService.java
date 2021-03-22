package com.michelin.ns4kafka.services.connect;


import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConfigInfos;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorInfo;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


@Singleton
public class KafkaConnectService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectService.class);

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;
    @Inject
    KafkaConnectClient kafkaConnectClient;

    public List<Connector> findByNamespace(String namespace) {

        String cluster = namespaceRepository.findByName(namespace).get().getCluster();

        List<AccessControlEntry> acls = accessControlEntryRepository.findAllGrantedToNamespace(namespace);

        return kafkaConnectClient.listAll(cluster)
                .entrySet()
                .stream()
                .filter(entry -> acls.stream()
                        .anyMatch(accessControlEntry -> {
                            //no need to check accessControlEntry.Permission, we want READ, WRITE or OWNER
                            if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT) {
                                switch (accessControlEntry.getSpec().getResourcePatternType()) {
                                    case PREFIXED:
                                        return entry.getKey().startsWith(accessControlEntry.getSpec().getResource());
                                    case LITERAL:
                                        return entry.getKey().equals(accessControlEntry.getSpec().getResource());
                                }
                            }
                            return false;
                        }))
                .map(entry -> Connector.builder()
                        .metadata(ObjectMeta.builder()
                                .name(entry.getKey())
                                .cluster(cluster)
                                .namespace(namespace)
                                .labels(Map.of("type", entry.getValue().getInfo().type().toString()))
                                .build())
                        .spec(entry.getValue().getInfo().config())
                        //TODO maybe map directly JSON response to this ?
                        /*.status(Connector.ConnectorStatus.builder()
                                .state(Connector.TaskState.valueOf(entry.getValue().getStatus().connector().state()))
                                .tasks(entry.getValue().getStatus().tasks()
                                        .stream()
                                        .map(task -> Connector.TaskStatus.builder()
                                                .id(String.valueOf(task.id()))
                                                .state(Connector.TaskState.valueOf(task.state()))
                                                .worker_id(task.workerId())
                                                .trace(task.trace())
                                                .build()
                                        )
                                        .collect(Collectors.toList())
                                )
                                .build()

                        )*/
                        .build()
                ).collect(Collectors.toList());
    }

    public Optional<Connector> findByName(String namespace, String connector) {
        return findByNamespace(namespace)
                .stream()
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .findFirst();
    }

    public List<String> validateLocally(String namespace, Connector connector) {
        Namespace ns = namespaceRepository.findByName(namespace).get();

        String connectorType = getConnectorType(namespace, connector.getSpec().get("connector.class"));

        //If class doesn't exist, no need to go further
        if (StringUtils.isEmpty(connectorType))
            return List.of("Failed to find any class that implements Connector and which name matches " +
                    connector.getSpec().get("connector.class"));

        //perform local validation
        List<String> validationErrors = ns.getConnectValidator().validate(connector, connectorType);

        return validationErrors;
    }

    public boolean isNamespaceOwnerOfConnect(String namespace, String connect) {
        return accessControlEntryRepository.findAllGrantedToNamespace(namespace)
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT)
                .anyMatch(accessControlEntry -> {
                    switch (accessControlEntry.getSpec().getResourcePatternType()) {
                        case PREFIXED:
                            return connect.startsWith(accessControlEntry.getSpec().getResource());
                        case LITERAL:
                            return connect.equals(accessControlEntry.getSpec().getResource());
                    }
                    return false;
                });
    }

    public List<String> validateRemotely(String namespace, Connector connector) {
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        // Calls the validate endpoints and returns the validation error messages if any
        ConfigInfos configInfos = kafkaConnectClient.validate(cluster, connector.getSpec().get("connector.class").toString(), connector.getSpec());

        return configInfos.values()
                .stream()
                .filter(configInfo -> !configInfo.configValue().errors().isEmpty())
                .flatMap(configInfo -> configInfo.configValue().errors().stream())
                .collect(Collectors.toList());
    }

    public Connector createOrUpdate(String namespace, Connector connector) {
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();

        ConnectorInfo connectorInfo = kafkaConnectClient.createOrUpdate(cluster, connector.getMetadata().getName(), connector.getSpec());

        return Connector.builder()
                .metadata(ObjectMeta.builder()
                        .name(connectorInfo.name())
                        .namespace(namespace)
                        .cluster(cluster)
                        .build())
                .spec(connectorInfo.config())
                .status(Connector.ConnectorStatus.builder()
                        .state(Connector.TaskState.UNASSIGNED) //or else ?
                        //.tasks(List.of(Tas))
                        .build())
                .build();
    }

    public String getConnectorType(String namespace, String connectorClass) {
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        return kafkaConnectClient.connectPlugins(cluster)
                .stream()

                .filter(connectPluginItem -> connectPluginItem.className().equals(connectorClass))
                .map(connectPluginItem -> connectPluginItem.type().toString())
                .findFirst()
                .orElse(null);
    }

    public HttpResponse delete(String namespace, String connector) {
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        return kafkaConnectClient.delete(cluster, connector);
    }
}
