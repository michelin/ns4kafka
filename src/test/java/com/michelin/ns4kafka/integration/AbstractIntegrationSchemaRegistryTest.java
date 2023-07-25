package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.testcontainers.SchemaRegistryContainer;
import io.micronaut.core.annotation.NonNull;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractIntegrationSchemaRegistryTest extends AbstractIntegrationTest {
    public static final String CONFLUENT_REGISTRY_VERSION = "7.4.1";

    public SchemaRegistryContainer schemaRegistryContainer;

    /**
     * Starts the Schema registry container
     * @return Properties enriched with the Schema Registry URL
     */
    @NonNull
    @Override
    public Map<String, String> getProperties() {
        Map<String, String> brokerProps = super.getProperties();
        if (schemaRegistryContainer == null || !schemaRegistryContainer.isRunning()) {
            schemaRegistryContainer = new SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_REGISTRY_VERSION),
                    "kafka:9092")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM", "PLAIN")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";")
                    .withNetwork(network);

            schemaRegistryContainer.start();
        }

        Map<String, String> properties = new HashMap<>(brokerProps);
        properties.put("ns4kafka.managed-clusters.test-cluster.schemaRegistry.url", schemaRegistryContainer.getUrl());
        return properties;
    }
}
