package com.michelin.ns4kafka.properties;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Introspected;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.admin.Admin;

/**
 * Managed cluster properties.
 */
@Getter
@Setter
@EachProperty("ns4kafka.managed-clusters")
public class ManagedClusterProperties {
    private String name;
    private boolean manageTopics;
    private boolean manageAcls;
    private boolean dropUnsyncAcls = true;
    private boolean manageUsers;
    private boolean manageConnectors;
    private KafkaProvider provider;
    private Properties config;
    private Map<String, ConnectProperties> connects;
    private SchemaRegistryProperties schemaRegistry;
    private Admin adminClient = null;

    public ManagedClusterProperties(@Parameter String name) {
        this.name = name;
    }

    public ManagedClusterProperties(@Parameter String name, @Parameter KafkaProvider provider) {
        this.name = name;
        this.provider = provider;
    }

    /**
     * Getter for admin client service.
     *
     * @return The admin client
     */
    public Admin getAdminClient() {
        if (this.adminClient == null) {
            this.adminClient = Admin.create(config);
        }

        return this.adminClient;
    }

    /**
     * Kafka provider.
     */
    public enum KafkaProvider {
        SELF_MANAGED,
        CONFLUENT_CLOUD
    }

    /**
     * Connect properties.
     */
    @Getter
    @Setter
    @Introspected
    public static class ConnectProperties {
        String url;
        String basicAuthUsername;
        String basicAuthPassword;
    }

    /**
     * Schema registry properties.
     */
    @Getter
    @Setter
    @ConfigurationProperties("schema-registry")
    public static class SchemaRegistryProperties {
        String url;
        String basicAuthUsername;
        String basicAuthPassword;
    }
}
