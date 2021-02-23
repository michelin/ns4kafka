package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.ConnectRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.kafka.DelayStartupListener;
import com.michelin.ns4kafka.validation.ResourceValidationException;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.reactivex.*;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.List;

@Tag(name = "Connects")
@Controller(value = "/api/namespaces/{namespace}/connects")
public class ConnectController {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectController.class);
    //TODO validate calls and forward to Connect REST API (sync ???)
    @Inject
    ConnectRepository connectRepository;
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;


    @Get("/")
    public Flowable<Connector> list(String namespace){

        return connectRepository.findByNamespace(namespace);
    }

    @Get("/{connector}")
    public Maybe<Connector> getConnector(String namespace, String connector){
        return connectRepository.findByNamespace(namespace)
                .filter(connect -> connect.getMetadata().getName().equals(connect))
                .firstElement();
    }

    @Post
    public Single<Connector> apply(String namespace, @Valid @Body Connector connector){

        LOG.debug("Beginning apply");

        Namespace ns = namespaceRepository.findByName(namespace).get();

        //2. Request is valid ?
        List<String> localValidationErrors = ns.getConnectValidator().validate(connector);

        LOG.debug("finished local validation");
        if(isNamespaceOwnerOfConnect(namespace,connector.getMetadata().getName())) {
            localValidationErrors.add("Invalid value " + connector.getMetadata().getName() + " for name: Namespace not OWNER of this connector");
        }

        Maybe<List<String>> remoteValidationErrors = connectRepository.validate(namespace, connector);


        return mergeValidationErrors(remoteValidationErrors,localValidationErrors)
                .flatMap(validationErrors -> {
                    if(validationErrors.size()>0){
                        return Single.error(new ResourceValidationException(validationErrors));
                    }else{
                        return createOrUpdateConnector(connector);
                    }
                });
    }
    private Single<Connector> createOrUpdateConnector(Connector connector){
        connector.setStatus(Connector.ConnectorStatus.builder().state(Connector.TaskState.RUNNING).build());
        return Single.just(connector);
    }
    private Single<List<String>> mergeValidationErrors(Maybe<List<String>> remoteValidationErrors, List<String> localValidationErrors){
        return remoteValidationErrors
                .mergeWith(Observable.just(localValidationErrors).firstElement())
                .flatMapIterable(strings -> strings)
                .toList();
    }
    private boolean isNamespaceOwnerOfConnect(String namespace, String connect) {
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

}
