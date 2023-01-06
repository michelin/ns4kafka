package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.testcontainers.KafkaConnectContainer;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIntegrationConnectTest extends AbstractIntegrationTest {
    public KafkaConnectContainer connect;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();
        Map<String, String> brokerProps = super.getProperties();

        if (connect == null || !connect.isRunning()) {
            //registry = new SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION), "kafka:9092");
            //registry.start();
            connect = new KafkaConnectContainer(DockerImageName.parse("confluentinc/cp-kafka-connect:" + CONFLUENT_VERSION),
                    "kafka:9092")
                    .withEnv("CONNECT_SASL_MECHANISM", "PLAIN")
                    .withEnv("CONNECT_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
                    .withEnv("CONNECT_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";")
                    .withNetwork(network);
            connect.start();
        }
        properties.putAll(brokerProps);
        properties.put("ns4kafka.managed-clusters.test-cluster.connects.test-connect.url", connect.getUrl());
        return properties;
    }
}
