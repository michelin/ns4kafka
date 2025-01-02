package com.michelin.ns4kafka.property;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Introspected;
import java.sql.Time;
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
    private boolean manageAcls;
    private boolean manageConnectors;
    private boolean manageTopics;
    private boolean manageUsers;
    private boolean dropUnsyncAcls = true;
    private TimeoutProperties timeout;
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
        private String url;
        private String basicAuthUsername;
        private String basicAuthPassword;
    }

    /**
     * Schema registry properties.
     */
    @Getter
    @Setter
    @ConfigurationProperties("schema-registry")
    public static class SchemaRegistryProperties {
        private String url;
        private String basicAuthUsername;
        private String basicAuthPassword;
    }

    /**
     * Timeout properties.
     */
    @Getter
    @Setter
    @ConfigurationProperties("timeout")
    public static class TimeoutProperties {
        private static final int DEFAULT_TIMEOUT_MS = 30000;
        private AclProperties acl;
        private TopicProperties topic;
        private UserProperties user;

        /**
         * ACL properties.
         */
        @Getter
        @Setter
        @ConfigurationProperties("acl")
        public static class AclProperties {
            private int describe = DEFAULT_TIMEOUT_MS;
            private int create = DEFAULT_TIMEOUT_MS;
            private int delete = DEFAULT_TIMEOUT_MS;
        }

        /**
         * Topic properties.
         */
        @Getter
        @Setter
        @ConfigurationProperties("topic")
        public static class TopicProperties {
            private int alterConfigs = DEFAULT_TIMEOUT_MS;
            private int create = DEFAULT_TIMEOUT_MS;
            private int describeConfigs = DEFAULT_TIMEOUT_MS;
            private int delete = DEFAULT_TIMEOUT_MS;
            private int list = DEFAULT_TIMEOUT_MS;
        }

        /**
         * User properties.
         */
        @Getter
        @Setter
        @ConfigurationProperties("user")
        public static class UserProperties {
            private int alterQuotas = DEFAULT_TIMEOUT_MS;
            private int alterScramCredentials = DEFAULT_TIMEOUT_MS;
            private int describeQuotas = DEFAULT_TIMEOUT_MS;
        }
    }

    /**
     * Check if the provider is Confluent Cloud.
     *
     * @return true if it is, false otherwise
     */
    public boolean isConfluentCloud() {
        return provider == KafkaProvider.CONFLUENT_CLOUD;
    }
}
