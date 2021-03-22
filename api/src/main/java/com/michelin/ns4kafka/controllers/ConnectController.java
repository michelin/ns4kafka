package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import com.michelin.ns4kafka.validation.ResourceValidationException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.*;
import io.micronaut.http.hateoas.JsonError;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.functions.Function;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.reactivestreams.Publisher;
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
    KafkaConnectService kafkaConnectService;
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;


    @Get("/")
    public Flowable<Connector> list(String namespace){

        return kafkaConnectService.findByNamespace(namespace);
    }

    @Get("/{connector}")
    public Maybe<Connector> getConnector(String namespace, String connector){
        return kafkaConnectService.findByNamespace(namespace)
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .firstElement();
    }

    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{connector}")
    public Single<HttpResponse> deleteConnector(String namespace, String connector){
        if(kafkaConnectService.isNamespaceOwnerOfConnect(namespace,connector)) {
            return kafkaConnectService.delete(namespace,connector)
                    .onErrorResumeNext(throwable ->{
                        //TODO better error handling plz, handle 404
                        return Single.error(new ConnectCreationException(throwable));
                    } );
        }else {
            return Single.error(new ResourceValidationException(List.of("Invalid value " + connector +
                    " for name: Namespace not OWNER of this connector")));
        }
    }

    @Post
    public Single<Connector> apply(String namespace, @Valid @Body Connector connector){

        LOG.debug("Beginning apply");

        Namespace ns = namespaceRepository.findByName(namespace).get();


        //1. Request is valid enough to perform local validation ?
        // we need :
        // - connector.class
        // - source/sink type (derived from connector.class on remote /connectors-plugins)
        //2. Validate locally

        Flowable<String> rxLocalValidationErrors = kafkaConnectService.validateLocally(namespace, connector);


        return rxLocalValidationErrors
                .toList()
                .flatMapPublisher(localValidationErrors -> {
                    if(localValidationErrors.isEmpty()){
                        // we have no local validation errors, move on to /validate endpoint on connect
                        return kafkaConnectService.validateLocally(namespace, connector);
                    }else{
                        // we have local validation errors, return just them
                        return Flowable.fromIterable(localValidationErrors);
                    }
                })
                .onErrorResumeNext((Function<? super Throwable, ? extends Publisher<? extends String>>) throwable ->
                        Flowable.just(throwable.getMessage())
                )
                .toList()
                .flatMap(validationErrors -> {
                    if(validationErrors.size()>0){
                        return Single.error(new ResourceValidationException(validationErrors));
                    }else{
                        return kafkaConnectService.createOrUpdate(namespace,connector)
                                .onErrorResumeNext((Function<? super Throwable, ? extends SingleSource<? extends Connector>>) throwable ->
                                        Single.error(new ConnectCreationException(throwable))
                                        );
                    }
                });
    }

    //TODO move elsewhere
    public static class ConnectCreationException extends RuntimeException {
        public ConnectCreationException(Throwable e){
            super(e);
        }
    }

    //TODO move elsewhere
    @Error(global = true)
    public HttpResponse<JsonError> validationExceptionHandler(HttpRequest request, ConnectCreationException e){
        return HttpResponse.badRequest()
                .body(new JsonError(e.getMessage()));
    }

}
