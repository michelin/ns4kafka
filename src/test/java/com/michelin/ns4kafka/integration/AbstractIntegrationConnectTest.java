package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.testcontainers.KafkaConnectContainer;
import io.micronaut.core.annotation.NonNull;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.utility.DockerImageName;

/**
 * Kafka Connect integration test.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIntegrationConnectTest extends AbstractIntegrationTest {
    public KafkaConnectContainer connectContainer;

    /**
     * Starts the Kafka Connect container.
     *
     * @return Properties enriched with the Kafka Connect URL
     */
    @NonNull
    @Override
    public Map<String, String> getProperties() {
        Map<String, String> brokerProps = super.getProperties();
        if (connectContainer == null || !connectContainer.isRunning()) {
            connectContainer =
                new KafkaConnectContainer(DockerImageName.parse("confluentinc/cp-kafka-connect:" + CONFLUENT_VERSION),
                    "kafka:9092")
                    .withEnv("CONNECT_SASL_MECHANISM", "PLAIN")
                    .withEnv("CONNECT_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
                    .withEnv("CONNECT_SASL_JAAS_CONFIG",
                        "org.apache.kafka.common.security.plain.PlainLoginModule "
                            + "required username=\"admin\" password=\"admin\";")
                    .withNetwork(network);
            connectContainer.start();
        }

        Map<String, String> properties = new HashMap<>(brokerProps);
        properties.put("ns4kafka.managed-clusters.test-cluster.connects.test-connect.url", connectContainer.getUrl());
        return properties;
    }
}
