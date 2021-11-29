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

    public static final String CONFLUENT_VERSION = "6.2.0";

    public KafkaContainer kafka;
    public Network network;
    private Admin adminClient;

    @NonNull
    @Override
    public Map<String, String> getProperties() {
        if (kafka == null || !kafka.isRunning()) {
            network = Network.newNetwork();
            kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
                    .withEnv(
                            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                            "PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT"
                    )
                    .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                    .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
                    .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN")
                    .withEnv("KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS", "PLAIN")

                    .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG",
                            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                    "username=\"admin\" password=\"admin\" " +
                                    "user_admin=\"admin\" " +
                                    "user_client=\"client\";")
                    .withEnv("KAFKA_SASL_JAAS_CONFIG",
                            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                    "username=\"admin\" password=\"admin\";")
                    .withEnv(
                            "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                    "username=\"admin\" password=\"admin\" " +
                                    "user_admin=\"admin\" " +
                                    "user_client=\"client\";")
                    .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.auth.SimpleAclAuthorizer")
                    .withEnv("KAFKA_SUPER_USERS", "User:admin")
                    .withNetworkAliases("kafka")
                    .withNetwork(network);
            kafka.start();
        }

        return Map.of(
                "kafka.bootstrap.servers", kafka.getHost()+":"+kafka.getMappedPort(9093),
                "kafka.sasl.mechanism", "PLAIN",
                "kafka.security.protocol", "SASL_PLAINTEXT",
                "kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";",

                "ns4kafka.managed-clusters.test-cluster.config.bootstrap.servers", kafka.getHost()+":"+kafka.getMappedPort(9093),
                "ns4kafka.managed-clusters.test-cluster.config.sasl.mechanism", "PLAIN",
                "ns4kafka.managed-clusters.test-cluster.config.security.protocol", "SASL_PLAINTEXT",
                "ns4kafka.managed-clusters.test-cluster.config.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";"
        );
    }

    public Admin getAdminClient() {
        if (adminClient == null)
            adminClient = Admin.create(Map.of(
                    "bootstrap.servers", kafka.getHost()+":"+kafka.getMappedPort(9093),
                    "sasl.mechanism", "PLAIN",
                    "security.protocol", "SASL_PLAINTEXT",
                    "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";"));
        return adminClient;
    }
}
