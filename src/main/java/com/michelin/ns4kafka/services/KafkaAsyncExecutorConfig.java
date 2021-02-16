package com.michelin.ns4kafka.services;


import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.convert.format.MapFormat;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import lombok.Getter;

import javax.inject.Inject;
import java.util.Map;

@Getter
@EachProperty("ns4kafka.managed-clusters")
public class KafkaAsyncExecutorConfig {
    private final String name;
    boolean manageTopics;
    boolean manageAcls;
    boolean manageUsers;
    boolean manageConnectors;
    boolean readOnly = true;
    String connectUrl;
    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    Map<String, Object> config;
    ConnectConfig connectConfig;
    RegistryConfig registryConfig;

    public KafkaAsyncExecutorConfig(@Parameter String name) {
        this.name = name;
    }

    @Getter
    @ConfigurationProperties("connect")
    public static class ConnectConfig {
        String url;
        String basicAuthUsername;
        String basicAuthPassword;
    }
    @Getter
    @ConfigurationProperties("schema-registry")
    public static class RegistryConfig {
        String url;
        String basicAuthUsername;
        String basicAuthPassword;
    }

}
