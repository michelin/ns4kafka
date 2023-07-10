package com.michelin.ns4kafka.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.admin.Admin;

import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@EachProperty("ns4kafka.managed-clusters")
public class KafkaAsyncExecutorConfig {
    private String name;
    private boolean manageTopics;
    private boolean manageAcls;
    private boolean dropUnsyncAcls = true;
    private boolean manageUsers;
    private boolean manageConnectors;
    private KafkaProvider provider;
    private Properties config;
    private Map<String, ConnectConfig> connects;
    private RegistryConfig schemaRegistry;
    private Admin adminClient = null;

    public KafkaAsyncExecutorConfig(@Parameter String name) {
        this.name = name;
    }

    public KafkaAsyncExecutorConfig(@Parameter String name, @Parameter KafkaProvider provider) {
        this.name = name;
        this.provider = provider;
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
    @Setter
    @ConfigurationProperties("schema-registry")
    public static class RegistryConfig {
        String url;
        String basicAuthUsername;
        String basicAuthPassword;
    }

    public enum KafkaProvider {
        SELF_MANAGED,
        CONFLUENT_CLOUD
    }

    /**
     * Getter for admin client service
     * @return The admin client
     */
    public Admin getAdminClient() {
        if (this.adminClient == null) {
            this.adminClient = Admin.create(config);
        }

        return this.adminClient;
    }
}
