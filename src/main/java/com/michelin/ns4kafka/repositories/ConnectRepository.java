package com.michelin.ns4kafka.repositories;


import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.ConnectRestService;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.inject.qualifiers.Qualifiers;
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
public class ConnectRepository {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectRepository.class);

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;
    @Inject
    ApplicationContext applicationContext;

    private ConnectRestService getConnectRestService(String namespace){
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        // retrive the ConnectRestService Bean byName(cluster)
        return applicationContext.getBean(
                ConnectRestService.class,
                Qualifiers.byName(cluster));
    }

    public Flowable<Connector> findByNamespace(String namespace){


        List<AccessControlEntry> acls = accessControlEntryRepository.findAllGrantedToNamespace(namespace);

        return getConnectRestService(namespace).list()
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
                                //TODO .cluster(cluster)
                                .namespace(namespace)
                                .labels(Map.of("type", entry.getValue().getInfo().getType()))
                                .build())
                        .spec(entry.getValue().getInfo().getConfig())
                        .status(Connector.ConnectorStatus.builder()
                                .state(Connector.TaskState.valueOf(entry.getValue().getStatus().getConnector().get("state")))
                                .tasks(entry.getValue().getStatus().getTasks()
                                        .stream()
                                        .map(task -> Connector.TaskStatus.builder()
                                                .id(task.get("id"))
                                                .state(Connector.TaskState.valueOf(task.get("state")))
                                                .worker_id(task.get("worker_id"))
                                                .trace(task.get("trace"))
                                                .build()
                                        )
                                        .collect(Collectors.toList())
                                )
                                .build()

                        )
                        .build()
                );
    }

    public Maybe<Connector> findByName(String namespace, String connector){
        return findByNamespace(namespace)
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .firstElement();
    }

    public Flowable<String> validate(String namespace, Connector connector){
        // Calls the validate endpoints and returns the validation error messages if any
        return getConnectRestService(namespace).validate(connector.getSpec())
                .flatMapIterable(connectValidationResult -> connectValidationResult
                        .getConfigs()
                        .stream()
                        .filter(connectValidationItem -> connectValidationItem.getValue().getErrors().size()>0)
                        .flatMap(connectValidationItem -> connectValidationItem.getValue().getErrors().stream())
                        .collect(Collectors.toList()
                        )
                );
    }

    public Single<Connector> createOrUpdate(String namespace, Connector connector){
        return getConnectRestService(namespace).createOrUpdate(connector)
                .map(connectInfo -> Connector.builder()
                        .metadata(ObjectMeta.builder()
                                .name(connectInfo.getName())
                                .namespace(namespace)
                                //.cluster(cluster)
                                .build())
                        .spec(connectInfo.getConfig())
                        .status(Connector.ConnectorStatus.builder()
                                .state(Connector.TaskState.UNASSIGNED) //or else ?
                                //.tasks(List.of(Tas))
                                .build())
                        .build()
                );
    }
    public Single<String> getConnectorType(String namespace, String connectorClass){
        return getConnectRestService(namespace).connectPlugins()
                .filter(connectPluginItem -> connectPluginItem.get_class().equals(connectorClass))
                .map(connectPluginItem -> connectPluginItem.getType())
                .singleOrError();
    }

    public Flowable<HttpResponse<String>> delete(String namespace, String connector) {
        return getConnectRestService(namespace).delete(connector);
    }
}
