package com.michelin.ns4kafka.services.executors;


import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.convert.format.MapFormat;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;

@Getter
@Setter
@EachProperty("ns4kafka.managed-clusters")
public class KafkaAsyncExecutorConfig {
    private final String name;
    boolean manageTopics;
    boolean manageAcls;
    boolean manageUsers;
    boolean manageConnectors;
    boolean manageStreams;
    boolean readOnly = true;
    
    Properties config;

    private Admin adminClient = null;

    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    Map<String, ConnectConfig> connects;

    RegistryConfig schemaRegistry;


    public KafkaAsyncExecutorConfig(@Parameter String name) {
        this.name = name;
    }

    @Getter
    @Setter
    @Introspected
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

    public Admin getAdminClient() {

        if(this.adminClient == null) {
            this.adminClient = Admin.create(config);
        }
        return this.adminClient;
    }

}
