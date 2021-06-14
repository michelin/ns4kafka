package com.michelin.ns4kafka.integration;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.test.support.TestPropertyProvider;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractIntegrationTest implements TestPropertyProvider {


    @NonNull
    @Override
    public Map<String, String> getProperties() {
        KafkaCluster.init();
        return Map.of(
                "kafka.bootstrap.servers", KafkaCluster.kafka.getBootstrapServers(),
                "ns4kafka.managed-clusters.test-cluster.config.bootstrap.servers", KafkaCluster.kafka.getBootstrapServers()
        );
    }
}
