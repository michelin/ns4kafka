package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Connector;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.Retryable;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.util.Map;

@Client(KafkaConnectClientProxy.CONNECT_PROXY_PREFIX)
public interface KafkaConnectClient {
    @Get("/connectors?expand=info&expand=status")
    void listAll(@Header(value = "X-Connect-Cluster") String cluster);

    @Put("/connector-plugins/{connectorClass}/config/validate")
    Flowable<ConnectRestService.ConnectValidationResult> validate(
            @Header(value = "X-Connect-Cluster") String cluster,
            String connectorClass,
            @Body Map<String,String> spec);

    @Retryable(predicate = ConnectRestService.RebalanceRetryPredicate.class)
    @Put("/connectors/{connector}/config")
    Single<ConnectRestService.ConnectInfo> createOrUpdate(
            @Header(value = "X-Connect-Cluster") String cluster,
            String connector,
            @Body Connector body);

    @Delete("/connectors/{connector}")
    HttpResponse delete(
            @Header(value = "X-Connect-Cluster") String cluster,
            String connector);


    @Get("/connector-plugins")
    Flowable<ConnectRestService.ConnectPluginItem> connectPlugins(
            @Header(value = "X-Connect-Cluster") String cluster);
}
