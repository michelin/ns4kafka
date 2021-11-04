package com.michelin.ns4kafka.integration;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.test.support.TestPropertyProvider;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIntegrationTest implements TestPropertyProvider {

    public static final String CONFLUENT_VERSION = "7.0.0";

    public KafkaContainer kafka;
    public Network network;
    private Admin adminClient;

    @NonNull
    @Override
    public Map<String, String> getProperties() {
        if (kafka == null || !kafka.isRunning()) {
            network = Network.newNetwork();
            kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
                    .withNetworkAliases("kafka")
                    .withNetwork(network);
            kafka.start();
        }

        return Map.of(
                "kafka.bootstrap.servers", kafka.getBootstrapServers(),
                "ns4kafka.managed-clusters.test-cluster.config.bootstrap.servers", kafka.getBootstrapServers()
        );
    }

    public Admin getAdminClient() {
        if (adminClient == null)
            adminClient = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()));
        return adminClient;
    }
}
