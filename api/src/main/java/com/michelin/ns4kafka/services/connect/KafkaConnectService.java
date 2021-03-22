package com.michelin.ns4kafka.services.connect;


import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import io.micronaut.http.HttpResponse;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
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

    public Flowable<Connector> findByNamespace(String namespace){

        String cluster = namespaceRepository.findByName(namespace).get().getCluster();

        List<AccessControlEntry> acls = accessControlEntryRepository.findAllGrantedToNamespace(namespace);

        return kafkaConnectClient.listAll(cluster)
                .toFlowable()
                .flatMapIterable(Map::entrySet)
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
                );
    }

    public Maybe<Connector> findByName(String namespace, String connector){
        return findByNamespace(namespace)
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .firstElement();
    }

    public Flowable<String> validateLocally(String namespace, Connector connector){
        Namespace ns = namespaceRepository.findByName(namespace).get();
        //retrives connectorType from class name
        Flowable<String> rxLocalValidationErrors = getConnectorType(namespace,connector.getSpec().get("connector.class"))
        //pass it to local validator
        .map(connectorType -> ns.getConnectValidator().validate(connector, connectorType))
        .flattenAsFlowable(strings -> strings)
        .onErrorReturn(throwable -> "Failed to find any class that implements Connector and which name matches "+connector.getSpec().get("connector.class"));

        //TODO refactor ?
        if(!isNamespaceOwnerOfConnect(namespace,connector.getMetadata().getName())) {
            rxLocalValidationErrors = rxLocalValidationErrors.concatWith(
                    Single.just("Invalid value " + connector.getMetadata().getName() +
                            " for name: Namespace not OWNER of this connector"));
        }
        return rxLocalValidationErrors;
    }

    public boolean isNamespaceOwnerOfConnect(String namespace, String connect) {
        return accessControlEntryRepository.findAllGrantedToNamespace(namespace)
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT)
                .anyMatch(accessControlEntry -> {
                    switch (accessControlEntry.getSpec().getResourcePatternType()){
                        case PREFIXED:
                            return connect.startsWith(accessControlEntry.getSpec().getResource());
                        case LITERAL:
                            return connect.equals(accessControlEntry.getSpec().getResource());
                    }
                    return false;
                });
    }
    public Flowable<String> validateRemotely(String namespace, Connector connector){
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        // Calls the validate endpoints and returns the validation error messages if any
        return kafkaConnectClient.validate(cluster,connector.getSpec().get("connector.class").toString(),connector.getSpec())
                .toFlowable()
                .flatMapIterable(configInfos -> configInfos
                        .values()
                        .stream()
                        .filter(configInfo -> ! configInfo.configValue().errors().isEmpty())
                        .flatMap(configInfo -> configInfo.configValue().errors().stream())
                        .collect(Collectors.toList())
                );
    }

    public Single<Connector> createOrUpdate(String namespace, Connector connector){
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        return kafkaConnectClient.createOrUpdate(cluster, connector.getMetadata().getName(), connector.getSpec())
                .map(connectInfo -> Connector.builder()
                        .metadata(ObjectMeta.builder()
                                .name(connectInfo.name())
                                .namespace(namespace)
                                .cluster(cluster)
                                .build())
                        .spec(connectInfo.config())
                        .status(Connector.ConnectorStatus.builder()
                                .state(Connector.TaskState.UNASSIGNED) //or else ?
                                //.tasks(List.of(Tas))
                                .build())
                        .build()
                );
    }
    public Single<String> getConnectorType(String namespace, String connectorClass){
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        return kafkaConnectClient.connectPlugins(cluster)
                .toFlowable()
                .flatMapIterable(connectorPluginInfos -> connectorPluginInfos)
                .filter(connectPluginItem -> connectPluginItem.className().equals(connectorClass))
                .map(connectPluginItem -> connectPluginItem.type().toString())
                .singleOrError();
    }

    public Single<HttpResponse> delete(String namespace, String connector) {
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        return kafkaConnectClient.delete(cluster,connector);
    }
}
