package com.michelin.ns4kafka.services.connect.client;

import com.michelin.ns4kafka.services.connect.KafkaConnectClientProxy;
import com.michelin.ns4kafka.services.connect.client.entities.ConfigInfos;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorInfo;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorPluginInfo;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStatus;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Single;

import java.util.List;
import java.util.Map;

@Client(KafkaConnectClientProxy.CONNECT_PROXY_PREFIX)
public interface KafkaConnectClient {
    @Get("/connectors?expand=info&expand=status")
    Single<Map<String, ConnectorStatus>> listAll(@Header(value = "X-Connect-Cluster") String cluster);

    @Put("/connector-plugins/{connectorClass}/config/validate")
    Single<ConfigInfos> validate(
            @Header(value = "X-Connect-Cluster") String cluster,
            String connectorClass,
            @Body Map<String,String> connectorSpec);

    //@Retryable(predicate = ConnectRestService.RebalanceRetryPredicate.class)
    @Put("/connectors/{connector}/config")
    Single<ConnectorInfo> createOrUpdate(
            @Header(value = "X-Connect-Cluster") String cluster,
            String connector,
            @Body Map<String,String> ConnectorSpec);

    @Delete("/connectors/{connector}")
    Single<HttpResponse> delete(
            @Header(value = "X-Connect-Cluster") String cluster,
            String connector);


    @Get("/connector-plugins")
    Single<List<ConnectorPluginInfo>> connectPlugins(
            @Header(value = "X-Connect-Cluster") String cluster);
}
