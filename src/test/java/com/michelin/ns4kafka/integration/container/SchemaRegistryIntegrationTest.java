package com.michelin.ns4kafka.integration.container;

import io.micronaut.core.annotation.NonNull;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for Schema Registry integration tests.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SchemaRegistryIntegrationTest extends KafkaIntegrationTest {
    private final GenericContainer<?> schemaRegistry = new GenericContainer<>(DockerImageName
        .parse("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION))
        .withNetwork(NETWORK)
        .withNetworkAliases("schema-registry")
        .withExposedPorts(8081)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:9093")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM", "PLAIN")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";")
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @NonNull
    @Override
    public Map<String, String> getProperties() {
        Map<String, String> brokerProperties = super.getProperties();

        if (!schemaRegistry.isRunning()) {
            schemaRegistry.start();
        }

        Map<String, String> properties = new HashMap<>(brokerProperties);
        properties.put("ns4kafka.managed-clusters.test-cluster.schemaRegistry.url", getSchemaRegistryUrl());
        return properties;
    }

    protected String getSchemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort();
    }
}
