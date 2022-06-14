package com.michelin.ns4kafka.services.executors;


import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.convert.format.MapFormat;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.admin.Admin;

import java.security.Provider;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@EachProperty("ns4kafka.managed-clusters")
public class KafkaAsyncExecutorConfig {
    /**
     * Cluster name
     */
    private final String name;

    /**
     * Run topics synchronization ?
     */
    boolean manageTopics;

    /**
     * Run ACLs synchronization ?
     */
    boolean manageAcls;

    /**
     * Drop unsynchronized ACLs ?
     */
    boolean dropUnsyncAcls = true;

    /**
     * Run users synchronization ?
     */
    boolean manageUsers;

    /**
     * Run connectors synchronization ?
     */
    boolean manageConnectors;

    /**
     * Kafka cluster provider
     */
    KafkaProvider provider;

    /**
     * Kafka cluster configuration
     */
    Properties config;

    /**
     * Kafka Connects configuration
     */
    @MapFormat(transformation = MapFormat.MapTransformation.FLAT)
    Map<String, ConnectConfig> connects;

    /**
     * Schema registry configuration
     */
    RegistryConfig schemaRegistry;

    /**
     * Admin client service
     */
    private Admin adminClient = null;

    /**
     * Constructor
     * @param name The cluster name
     */
    public KafkaAsyncExecutorConfig(@Parameter String name) {
        this.name = name;
    }

    /**
     * Constructor
     * @param name The cluster name
     * @param provider The kafka provider
     */
    public KafkaAsyncExecutorConfig(@Parameter String name, @Parameter KafkaProvider provider) {
        this.name = name;
        this.provider = provider;
    }

    @Getter
    @Setter
    @Introspected
    public static class ConnectConfig {
        /**
         * Kafka Connect URL
         */
        String url;

        /**
         * Kafka Connect username
         */
        String basicAuthUsername;

        /**
         * Kafka Connect password
         */
        String basicAuthPassword;
    }

    @Getter
    @Setter
    @ConfigurationProperties("schema-registry")
    public static class RegistryConfig {
        /**
         * Schema Registry URL
         */
        String url;

        /**
         * Schema Registry username
         */
        String basicAuthUsername;

        /**
         * Schema Registry password
         */
        String basicAuthPassword;
    }

    /**
     * Kafka cluster provider type
     */
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
