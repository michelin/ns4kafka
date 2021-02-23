package com.michelin.ns4kafka.repositories;


import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.kafka.KafkaStore;
import com.michelin.ns4kafka.services.ConnectRestService;
import com.michelin.ns4kafka.services.KafkaAsyncExecutor;
import com.michelin.ns4kafka.services.KafkaAsyncExecutorConfig;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.RxHttpClientFactory;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.retry.annotation.Retryable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
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

    public Flowable<Connector> findByNamespace(String namespace){
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        // retrive the ConnectRestService Bean byName(cluster)
        ConnectRestService connectRestService = applicationContext.getBean(
                ConnectRestService.class,
                Qualifiers.byName(cluster));

        List<AccessControlEntry> acls = accessControlEntryRepository.findAllGrantedToNamespace(namespace);

        return connectRestService.list()
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
        LOG.debug("findbyName("+namespace+","+connector+")");
        return findByNamespace(namespace)
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .doOnNext(connector1 -> LOG.debug("findByName found "+connector1.getMetadata().getName()))
                .firstElement();
    }

    public Maybe<List<String>> validate(String namespace, Connector connector){
        LOG.debug("Starting validate");
        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        // Retrieves the ConnectRestService Bean byName(cluster)
        ConnectRestService connectRestService = applicationContext.getBean(
                ConnectRestService.class,
                Qualifiers.byName(cluster));
        // Calls the validate endpoints and returns the validation error messages if any
        return connectRestService.validate(connector.getSpec())
                .doOnEvent((connectValidationResult, throwable) -> LOG.debug("Result there during validate"))
                .map(connectValidationResult -> connectValidationResult
                        .getConfigs()
                        .stream()
                        .filter(connectValidationItem -> connectValidationItem.getValue().getErrors().size()>0)
                        .flatMap(connectValidationItem -> connectValidationItem.getValue().getErrors().stream())
                        .collect(Collectors.toList())
                );
    }
}
