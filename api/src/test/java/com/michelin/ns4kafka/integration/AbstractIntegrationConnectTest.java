package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.testcontainers.KafkaConnectContainer;
import com.michelin.ns4kafka.testcontainers.SchemaRegistryContainer;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIntegrationConnectTest extends AbstractIntegrationTest {

    public SchemaRegistryContainer registry;
    public KafkaConnectContainer connect;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();
        Map<String, String> brokerProps = super.getProperties();

        if (connect == null || !connect.isRunning()) {
            //registry = new SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION), "kafka:9092");
            //registry.start();
            connect = new KafkaConnectContainer(DockerImageName.parse("confluentinc/cp-kafka-connect:" + CONFLUENT_VERSION), "kafka:9092")
                    .withNetwork(network);
            connect.start();
        }
        properties.putAll(brokerProps);
        properties.put("ns4kafka.managed-clusters.test-cluster.config.connects.test-connect.url", connect.getUrl());
        return properties;
    }
}
