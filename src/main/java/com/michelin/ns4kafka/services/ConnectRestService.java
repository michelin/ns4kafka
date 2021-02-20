package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.ConnectRepository;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class ConnectRestService {

    RxHttpClient httpClient;

    public ConnectRestService(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        try {
            if(kafkaAsyncExecutorConfig.getConnect()!=null) {
                httpClient = RxHttpClient.create(new URL(kafkaAsyncExecutorConfig.getConnect().getUrl()));
            }
        }catch (MalformedURLException e){

        }
    }

    public Maybe<Map<String,ConnectItem>> list() {
        return httpClient.retrieve(HttpRequest.GET("connectors?expand=info&expand=status"),
                Argument.mapOf(String.class, ConnectItem.class))
                .firstElement();
    }


    /**
     * These objects are mapped from JSON responses from the Connect Rest API
     */
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
