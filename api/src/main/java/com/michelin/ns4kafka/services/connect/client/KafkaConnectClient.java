package com.michelin.ns4kafka.services.connect.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.services.connect.KafkaConnectClientProxy;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;
import lombok.Getter;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorPluginInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;

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
    ConfigInfos validate(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connectorClass,
            @Body Map<String,String> connectorSpec);

    //@Retryable(predicate = ConnectRestService.RebalanceRetryPredicate.class)
    @Put("/connectors/{connector}/config")
    ConnectorInfo createOrUpdate(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector,
            @Body Map<String,String> ConnectorSpec);

    @Delete("/connectors/{connector}")
    HttpResponse delete(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector);


    @Get("/connector-plugins")
    List<ConnectorPluginInfo> connectPlugins(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster);

    @Get("/connectors/{connector}/status")
    ConnectorStateInfo status(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector);

    @Post("/connectors/{connector}/tasks/{taskid}/restart")
    HttpResponse restart(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector,
            int taskid);

    @Put("/connectors/{connector}/pause")
    HttpResponse pause(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector
    );

    @Put("/connectors/{connector}/resume")
    HttpResponse resume(
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_SECRET) String secret,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
            @Header(value = KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER) String connectCluster,
            String connector
    );

    /**
     * Object is not defined in org.apache.kafka.connect, as it is the result of a particular call
     * kafka-connect:8083/connectors?expand=info&expand=status
     */
    @Getter
    class ConnectorStatus {
        private final ConnectorInfo info;
        private final ConnectorStateInfo status;
        @JsonCreator
        public ConnectorStatus(@JsonProperty("info") ConnectorInfo info, @JsonProperty("status") ConnectorStateInfo status){
            this.info=info;
            this.status=status;
        }
    }
}
