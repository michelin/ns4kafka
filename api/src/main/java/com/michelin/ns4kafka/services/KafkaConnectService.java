package com.michelin.ns4kafka.services;


import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConfigInfos;
import com.michelin.ns4kafka.services.executors.ConnectorAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;


@Singleton
public class KafkaConnectService {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectService.class);

    @Inject
    AccessControlEntryService accessControlEntryService;
    @Inject
    KafkaConnectClient kafkaConnectClient;
    @Inject
    ConnectorRepository connectorRepository;
    @Inject
    ApplicationContext applicationContext;

    public List<Connector> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace);
        return connectorRepository.findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(connector -> acls.stream().anyMatch(accessControlEntry -> {
                    //need to check accessControlEntry.Permission, we want OWNER
                    if (accessControlEntry.getSpec().getPermission() != AccessControlEntry.Permission.OWNER) {
                        return false;
                    }
                    if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT) {
                        switch (accessControlEntry.getSpec().getResourcePatternType()) {
                            case PREFIXED:
                                return connector.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                            case LITERAL:
                                return connector.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                        }
                    }
                    return false;
                }))
                .collect(Collectors.toList());
    }

    public Optional<Connector> findByName(Namespace namespace, String connector) {
        return findAllForNamespace(namespace)
                .stream()
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .findFirst();
    }

    public List<String> validateLocally(Namespace namespace, Connector connector) {

        //check whether target Connect Cluster is allowed for this namespace
        if(!namespace.getSpec().getConnectClusters().contains(connector.getSpec().getConnectCluster())){
            String allowedConnectClusters = String.join(", ",namespace.getSpec().getConnectClusters());
            return List.of("Invalid value " + connector.getSpec().getConnectCluster() + " for spec.connectCluster: Value must be one of ["+allowedConnectClusters+"]");
        }

        //If class doesn't exist, no need to go further
        if (StringUtils.isEmpty(connector.getSpec().getConfig().get("connector.class")))
            return List.of("Invalid value for spec.config.'connector.class': Value must be non-null");

        // Connector type exists on this target connect cluster ?
        Optional<String> connectorType = kafkaConnectClient.connectPlugins(namespace.getMetadata().getCluster(), connector.getSpec().getConnectCluster())
                .stream()
                .filter(connectPluginItem -> connectPluginItem.className().equals(connector.getSpec().getConfig().get("connector.class")))
                .map(connectorPluginInfo -> connectorPluginInfo.type().toString().toLowerCase(Locale.ROOT))
                .findFirst();
        if(connectorType.isEmpty()){
            return List.of("Failed to find any class that implements Connector and which name matches " +
                    connector.getSpec().getConfig().get("connector.class"));
        }

        //perform local validation
        List<String> validationErrors = namespace.getSpec().getConnectValidator().validate(connector, connectorType.get());

        return validationErrors;
    }

    public boolean isNamespaceOwnerOfConnect(Namespace namespace, String connect) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(), AccessControlEntry.ResourceType.CONNECT, connect);
    }

    public List<String> validateRemotely(Namespace namespace, Connector connector) {
        // Calls the validate endpoints and returns the validation error messages if any
        ConfigInfos configInfos = kafkaConnectClient.validate(
                namespace.getMetadata().getCluster(),
                connector.getSpec().getConnectCluster(),
                connector.getSpec().getConfig().get("connector.class"),
                connector.getSpec().getConfig());

        return configInfos.values()
                .stream()
                .filter(configInfo -> !configInfo.configValue().errors().isEmpty())
                .flatMap(configInfo -> configInfo.configValue().errors().stream())
                .collect(Collectors.toList());
    }

    public Connector createOrUpdate(Namespace namespace, Connector connector) {
        return connectorRepository.create(connector);
    }

    public HttpResponse delete(Namespace namespace, Connector connector) {
        connectorRepository.delete(connector);
        return kafkaConnectClient.delete(
                namespace.getMetadata().getCluster(),
                connector.getSpec().getConnectCluster(),
                connector.getMetadata().getName());
    }

    public List<Connector> listUnsynchronizedConnectors(Namespace namespace) {
        ConnectorAsyncExecutor connectorAsyncExecutor = applicationContext.getBean(ConnectorAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        return namespace.getSpec().getConnectClusters().stream()
                // get all connectors from all connect clusters...
                .flatMap(connectCluster -> connectorAsyncExecutor.collectBrokerConnectors(connectCluster).stream())
                // ...that belongs to this namespace
                .filter(connector -> isNamespaceOwnerOfConnect(namespace, connector.getMetadata().getName()))
                // ...and aren't in ns4kafka storage
                .filter(connector -> findByName(namespace, connector.getMetadata().getName()).isEmpty())
                .collect(Collectors.toList());
    }
}
