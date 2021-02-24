package com.michelin.ns4kafka.services;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.repositories.ConnectRepository;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.retry.annotation.DefaultRetryPredicate;
import io.micronaut.retry.annotation.Retryable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

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

    public Flowable<ConnectValidationResult> validate(Map<String,String> spec){
        return httpClient.retrieve(
                HttpRequest.PUT("connector-plugins/"+spec.get("connector.class")+"/config/validate", spec),
                ConnectValidationResult.class)
                .subscribeOn(Schedulers.io());
    }

    @Retryable(predicate = RebalanceRetryPredicate.class)
    public Single<ConnectInfo> createOrUpdate(Connector connector){
        //TODO real call
        /*
        return httpClient.retrieve(
                HttpRequest.PUT("connectors/"+connector.getMetadata().getName()+"/config",connector.getSpec()),
                ConnectInfo.class)
                .subscribeOn(Schedulers.io())
                .singleOrError();
        */
        return Single.error(new HttpClientResponseException(
                "Cannot complete request because of a conflicting operation (e.g. worker rebalance)",
                HttpResponse.status(HttpStatus.valueOf(409))));
    }

    public static class RebalanceRetryPredicate extends DefaultRetryPredicate{
        @Override
        public boolean test(Throwable throwable) {
            if (throwable instanceof HttpClientResponseException) {
                HttpClientResponseException e = (HttpClientResponseException) throwable;
                return e.getStatus().getCode() == 409;
            }
            return false;
        }
    }

    public Flowable<ConnectPluginItem> connectPlugins(){
        return httpClient.retrieve(HttpRequest.GET("connector-plugins"),
                Argument.listOf(ConnectPluginItem.class))
                .subscribeOn(Schedulers.io())
                .flatMapIterable(connectPluginItems -> connectPluginItems);
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
    @Getter
    @Setter
    @NoArgsConstructor
    public static class ConnectPluginItem{
        @JsonProperty("class")
        String _class;
        String type;

    }
}
