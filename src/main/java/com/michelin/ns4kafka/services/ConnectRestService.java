package com.michelin.ns4kafka.services;

import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.RxHttpClient;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.schedulers.Schedulers;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

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
    @PreDestroy
    public void cleanup(){
        httpClient.close();
    }

    //TODO
    // Caching (10-20seconds or something)
    // https://guides.micronaut.io/micronaut-cache/guide/index.html
    public Maybe<Map<String,ConnectItem>> list() {
        return httpClient.retrieve(HttpRequest.GET("connectors?expand=info&expand=status"),
                Argument.mapOf(String.class, ConnectItem.class))
                .firstElement()
                .subscribeOn(Schedulers.io());
    }

    public Maybe<ConnectValidationResult> validate(Map<String,String> spec){
        return httpClient.retrieve(HttpRequest.PUT("connector-plugins/"+spec.get("connector.class")+"/config/validate", spec),
                ConnectValidationResult.class)
                .firstElement()
                .subscribeOn(Schedulers.io());
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
    @Getter
    @Setter
    @NoArgsConstructor
    public static class ConnectValidationResult{
        String name;
        int error_count;
        List<ConnectValidationItem> configs;
    }
    @Getter
    @Setter
    @NoArgsConstructor
    public static class ConnectValidationItem{
        //Object definition;
        ConnectValidationItemValue value;

    }
    @Getter
    @Setter
    @NoArgsConstructor
    public static class ConnectValidationItemValue{
        String name;
        String value;
        List<String> recommended_values;
        List<String> errors;
        boolean visible;
    }
}
