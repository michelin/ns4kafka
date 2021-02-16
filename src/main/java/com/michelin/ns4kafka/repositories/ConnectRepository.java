package com.michelin.ns4kafka.repositories;


import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.KafkaAsyncExecutorConfig;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.RxHttpClientFactory;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


@Singleton
public class ConnectRepository {

    @Inject
    @Client("/")
    RxHttpClient httpClient;

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;


    public Maybe<Map<String,ConnectItem>> list(String namespace){
        Optional<Namespace> namespaceOptional = namespaceRepository.findByName(namespace);
        List<AccessControlEntry> acls = accessControlEntryRepository.findAllGrantedToNamespace(namespace);
        return httpClient.retrieve(HttpRequest.GET("https://localhost/connectors?expand=info&expand=status"),
                Argument.mapOf(String.class,ConnectItem.class))
                .flatMapIterable(stringConnectItemMap -> stringConnectItemMap.entrySet())
                .filter(stringConnectItemEntry -> stringConnectItemEntry.getKey().contains("jujuju"))
                .toMap(stringConnectItemEntry -> stringConnectItemEntry.getKey(),stringConnectItemEntry -> stringConnectItemEntry.getValue())
                .toMaybe();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class ConnectItem {
        ConnectInfo info;
        ConnectStatus status;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class ConnectInfo {
        String name;
        String type;
        Map<String,String> config;
        List<Map<String,String>> tasks;
    }
    @Getter
    @Setter
    @NoArgsConstructor
    public static class ConnectStatus{
        String name;
        String type;
        Map<String,String> connector;
        List<Map<String,String>> tasks;
    }

}
