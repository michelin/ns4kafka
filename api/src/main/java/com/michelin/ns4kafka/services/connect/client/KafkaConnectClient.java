package com.michelin.ns4kafka.services.connect.client;

import com.michelin.ns4kafka.services.connect.KafkaConnectClientProxy;
import com.michelin.ns4kafka.services.connect.client.entities.*;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Single;

import java.util.List;
import java.util.Map;

@Client(KafkaConnectClientProxy.PROXY_PREFIX)
public interface KafkaConnectClient {
    @Get("/connectors?expand=info&expand=status")
    Map<String, ConnectorStatus> listAll(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster);

    @Put("/connector-plugins/{connectorClass}/config/validate")
    Single<ConfigInfos> validate(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connectorClass,
            @Body ConnectorSpecs connectorSpec);

    @Put("/connectors/{connector}/config")
    ConnectorInfo createOrUpdate(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector,
            @Body ConnectorSpecs connectorSpec);

    @Delete("/connectors/{connector}")
    Single<HttpResponse<Void>> delete(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector);


    @Get("/connector-plugins")
    Single<List<ConnectorPluginInfo>> connectPlugins(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster);

    @Get("/connectors/{connector}/status")
    Single<ConnectorStateInfo> status(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector);

    @Post("/connectors/{connector}/tasks/{taskId}/restart")
    Single<HttpResponse<Void>> restart(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector,
            int taskId);

    @Put("/connectors/{connector}/pause")
    Single<HttpResponse<Void>> pause(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector
    );

    @Put("/connectors/{connector}/resume")
    Single<HttpResponse<Void>> resume(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector
    );
}
