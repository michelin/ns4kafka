package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectClientProxy;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.executors.ConnectorAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.reactivex.Observable;
import io.reactivex.Single;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;


@Slf4j
@Singleton
public class KafkaConnectService {
    /**
     * The ACL service
     */
    @Inject
    AccessControlEntryService accessControlEntryService;

    /**
     * The connector HTTP client
     */
    @Inject
    KafkaConnectClient kafkaConnectClient;

    /**
     * The connector repository
     */
    @Inject
    ConnectorRepository connectorRepository;

    /**
     * The application context
     */
    @Inject
    ApplicationContext applicationContext;

    /**
     * Find all connectors by given namespace
     * @param namespace The namespace
     * @return A list of connectors
     */
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

    /**
     * Find a connector by namespace and name
     * @param namespace The namespace
     * @param connector The connector name
     * @return An optional connector
     */
    public Optional<Connector> findByName(Namespace namespace, String connector) {
        return findAllForNamespace(namespace)
                .stream()
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .findFirst();
    }

    /**
     * Validate configurations of a given connector against the namespace rules
     * @param namespace The namespace
     * @param connector The connector to validate
     * @return A list of errors
     */
    public Single<List<String>> validateLocally(Namespace namespace, Connector connector) {
        // Check whether target Connect Cluster is allowed for this namespace
        if(!namespace.getSpec().getConnectClusters().contains(connector.getSpec().getConnectCluster())){
            String allowedConnectClusters = String.join(", ",namespace.getSpec().getConnectClusters());
            return Single.just(
                    List.of("Invalid value " + connector.getSpec().getConnectCluster() + " for spec.connectCluster: Value must be one of ["+allowedConnectClusters+"]"));
        }

        // If class doesn't exist, no need to go further
        if (StringUtils.isEmpty(connector.getSpec().getConfig().get("connector.class")))
            return Single.just(List.of("Invalid value for spec.config.'connector.class': Value must be non-null"));

        // Connector type exists on this target connect cluster ?
        return kafkaConnectClient.connectPlugins(KafkaConnectClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster())
                .map(connectorPluginInfos -> {
                    Optional<String> connectorType = connectorPluginInfos
                            .stream()
                            .filter(connectPluginItem -> connectPluginItem.className().equals(connector.getSpec().getConfig().get("connector.class")))
                            .map(connectorPluginInfo -> connectorPluginInfo.type().toString().toLowerCase(Locale.ROOT))
                            .findFirst();

                    if (connectorType.isEmpty()) {
                        return List.of("Failed to find any class that implements Connector and which name matches " +
                                connector.getSpec().getConfig().get("connector.class"));
                    }

                    return namespace.getSpec().getConnectValidator().validate(connector, connectorType.get());
                });
    }

    /**
     * Is given namespace owner of the given connector
     * @param namespace The namespace
     * @param connect The connector
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfConnect(Namespace namespace, String connect) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(), AccessControlEntry.ResourceType.CONNECT, connect);
    }

    /**
     * Validate configurations of a given connector against the cluster
     * @param namespace The namespace
     * @param connector The connector
     * @return A list of errors
     */
    public Single<List<String>> validateRemotely(Namespace namespace, Connector connector) {
        // Calls the "validate" endpoints and returns the validation error messages if any
        return kafkaConnectClient.validate(
                KafkaConnectClientProxy.PROXY_SECRET,
                namespace.getMetadata().getCluster(),
                connector.getSpec().getConnectCluster(),
                connector.getSpec().getConfig().get("connector.class"),
                ConnectorSpecs.builder()
                        .config(connector.getSpec().getConfig())
                        .build())
                .map(configInfos -> configInfos.values()
                        .stream()
                        .filter(configInfo -> !configInfo.configValue().errors().isEmpty())
                        .flatMap(configInfo -> configInfo.configValue().errors().stream())
                        .collect(Collectors.toList()));
    }

    /**
     * Create a given connector
     * @param connector The connector to create
     * @return The created connector
     */
    public Connector createOrUpdate(Connector connector) {
        return connectorRepository.create(connector);
    }

    /**
     * Delete a given connector
     * @param namespace The namespace
     * @param connector The connector
     */
    public Single<HttpResponse<Void>> delete(Namespace namespace, Connector connector) {
        return kafkaConnectClient.delete(KafkaConnectClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        connector.getSpec().getConnectCluster(), connector.getMetadata().getName())
                .defaultIfEmpty(HttpResponse.noContent())
                .flatMapSingle(httpResponse -> {
                    connectorRepository.delete(connector);

                    if (log.isInfoEnabled()) {
                        log.info("Success removing Connector [" + connector.getMetadata().getName() +
                                "] on Kafka [" + namespace.getMetadata().getName() +
                                "] Connect [" + connector.getSpec().getConnectCluster() + "]");
                    }

                    return Single.just(httpResponse);
                });
    }

    /**
     * List the connectors that are not synchronized to ns4kafka by namespace
     * @param namespace The namespace
     * @return The list of connectors
     */
    public Single<List<Connector>> listUnsynchronizedConnectors(Namespace namespace) {
        ConnectorAsyncExecutor connectorAsyncExecutor = applicationContext.getBean(ConnectorAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        // Get all connectors from all connect clusters
        List<Observable<Connector>> connectors = namespace.getSpec().getConnectClusters().stream()
                .map(connectCluster -> connectorAsyncExecutor.collectBrokerConnectors(connectCluster)
                        .toObservable()
                        .flatMapIterable(brokerConnectors -> brokerConnectors)
                        // That belongs to this namespace
                        .filter(connector -> isNamespaceOwnerOfConnect(namespace, connector.getMetadata().getName()))
                        // And aren't in ns4kafka storage
                        .filter(connector -> findByName(namespace, connector.getMetadata().getName()).isEmpty()))
                .collect(Collectors.toList());

        return Observable.merge(connectors).toList();
    }

    /**
     * Restart a given connector
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Single<HttpResponse<Void>> restart(Namespace namespace, Connector connector) {
        return kafkaConnectClient.status(KafkaConnectClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), connector.getSpec().getConnectCluster(),
                connector.getMetadata().getName())
                .flatMap(status -> {
                    Observable<HttpResponse<Void>> observable = Observable.fromIterable(status.tasks())
                        .flatMapSingle(task -> kafkaConnectClient.restart(KafkaConnectClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                                connector.getSpec().getConnectCluster(), connector.getMetadata().getName(), task.id()))
                        .map(restartedTasks -> {
                            log.info("Success restarting connector [{}] on namespace [{}] connect [{}]",
                                    connector.getMetadata().getName(),
                                    namespace.getMetadata().getName(),
                                    connector.getSpec().getConnectCluster());

                            return HttpResponse.ok();
                        });

                    return Single.fromObservable(observable);
                });
    }

    /**
     * Pause a given connector
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Single<HttpResponse<Void>> pause(Namespace namespace, Connector connector) {
        return kafkaConnectClient.pause(KafkaConnectClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                connector.getSpec().getConnectCluster(), connector.getMetadata().getName())
                        .map(pause -> {
                            log.info("Success pausing Connector [{}] on Namespace [{}] Connect [{}]",
                                    connector.getMetadata().getName(),
                                    namespace.getMetadata().getName(),
                                    connector.getSpec().getConnectCluster());

                            return HttpResponse.accepted();
                        });
    }

    /**
     * Resume a given connector
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Single<HttpResponse<Void>> resume(Namespace namespace, Connector connector) {
        return kafkaConnectClient.resume(KafkaConnectClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                connector.getSpec().getConnectCluster(), connector.getMetadata().getName())
                        .map(resume -> {
                            log.info("Success resuming Connector [{}] on Namespace [{}] Connect [{}]",
                                    connector.getMetadata().getName(),
                                    namespace.getMetadata().getName(),
                                    connector.getSpec().getConnectCluster());

                            return HttpResponse.accepted();
                        });
    }
}
