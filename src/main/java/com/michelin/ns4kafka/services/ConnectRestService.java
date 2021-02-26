package com.michelin.ns4kafka.services;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.models.Connector;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
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
    KafkaAsyncExecutorConfig.ConnectConfig connectConfig;

    public ConnectRestService(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        try {
            if(kafkaAsyncExecutorConfig.getConnect()!=null) {
                this.connectConfig = kafkaAsyncExecutorConfig.getConnect();
                this.httpClient = RxHttpClient.create(new URL(kafkaAsyncExecutorConfig.getConnect().getUrl()));
            }
        }catch (MalformedURLException e){

        }
    }
    @PreDestroy
    public void cleanup(){
        httpClient.close();
    }

    public <I, O> Flowable<O> retrieve(MutableHttpRequest<I> request, Argument<O> bodyType) {
        if(StringUtils.isNotEmpty(connectConfig.getBasicAuthUsername())){
            request = request.basicAuth(connectConfig.getBasicAuthUsername(),connectConfig.getBasicAuthPassword());
        }
        return httpClient.retrieve(request, bodyType)
                .subscribeOn(Schedulers.io());
    }
    public <I, O> Flowable<HttpResponse<O>> exchange(MutableHttpRequest<I> request, Argument<O> bodyType) {
        if(StringUtils.isNotEmpty(connectConfig.getBasicAuthUsername())){
            request = request.basicAuth(connectConfig.getBasicAuthUsername(),connectConfig.getBasicAuthPassword());
        }
        return httpClient.exchange(request, bodyType)
                .subscribeOn(Schedulers.io());
    }

    //TODO
    // Caching (10-20seconds or something)
    // https://guides.micronaut.io/micronaut-cache/guide/index.html
    public Maybe<Map<String,ConnectItem>> list() {
        return retrieve(HttpRequest.GET("connectors?expand=info&expand=status"),
                Argument.mapOf(String.class, ConnectItem.class))
                .firstElement();
    }

    public Flowable<ConnectValidationResult> validate(Map<String,String> spec){
        return retrieve(
                HttpRequest.PUT("connector-plugins/"+spec.get("connector.class")+"/config/validate", spec),
                Argument.of(ConnectValidationResult.class)
        );
    }

    @Retryable(predicate = RebalanceRetryPredicate.class)
    public Single<ConnectInfo> createOrUpdate(Connector connector){
        return retrieve(
                HttpRequest.PUT("connectors/"+connector.getMetadata().getName()+"/config",connector.getSpec()),
                Argument.of(ConnectInfo.class))
                .singleOrError();
    }

    public Flowable<HttpResponse<String>> delete(String connector){
        return exchange(
                HttpRequest.DELETE("connectors/"+connector),
                Argument.STRING);

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
