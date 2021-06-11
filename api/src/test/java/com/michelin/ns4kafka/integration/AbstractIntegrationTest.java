package com.michelin.ns4kafka.integration;

import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.runtime.server.EmbeddedServer;

public abstract class AbstractIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"))
            .withEmbeddedZookeeper()
            .withExposedPorts(9093);


    static RxHttpClient client;
    static EmbeddedServer server;

    @BeforeAll
    public static void initUnitTest() throws InterruptedException {
        kafka.start();
        server = ApplicationContext.run(EmbeddedServer.class, PropertySource.of(
                "test", Map.of(
                        "kafka.bootstrap.servers", kafka.getBootstrapServers(),
                        "ns4kafka.managed-clusters.test-cluster.config.bootstrap.servers", kafka.getBootstrapServers()
                )), "test");

        client = RxHttpClient.create(server.getURL());
    }

}
