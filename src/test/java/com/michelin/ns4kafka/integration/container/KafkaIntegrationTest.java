package com.michelin.ns4kafka.integration.container;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.test.support.TestPropertyProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for Kafka integration tests.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class KafkaIntegrationTest implements TestPropertyProvider {
    protected static final String CONFLUENT_PLATFORM_VERSION = "7.7.0";
    protected static final Network NETWORK = Network.newNetwork();
    private Admin adminClient;

    protected final KafkaContainer broker = new KafkaContainer(DockerImageName
        .parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
        .withNetwork(NETWORK)
        .withNetworkAliases("broker")
        .withCreateContainerCmdModifier(modifier -> {
            List<String> commands = new ArrayList<>(Arrays.asList(Objects.requireNonNull(modifier.getCmd())));
            // Override the security protocol for CONTROLLER listener to SASL_PLAINTEXT defined by
            // KafkaContainer#withRaft.
            commands.set(1, "export KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=BROKER:SASL_PLAINTEXT,"
                + "PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:SASL_PLAINTEXT; " + commands.get(1));
            modifier.withCmd(commands);
        })
        .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
        .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
        .withEnv("KAFKA_SASL_MECHANISM_CONTROLLER_PROTOCOL", "PLAIN")
        .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG", getSaslPlainJaasConfig())
        .withEnv("KAFKA_LISTENER_NAME_CONTROLLER_PLAIN_SASL_JAAS_CONFIG", getSaslPlainJaasConfig())
        .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", getSaslPlainJaasConfig())
        .withEnv("KAFKA_SUPER_USERS", "User:admin")
        .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
        .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
        .withKraft();

    @NonNull
    @Override
    public Map<String, String> getProperties() {
        if (!broker.isRunning()) {
            broker.start();
        }

        return Map.of(
            "kafka." + BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers(),
            "ns4kafka.managed-clusters.test-cluster.config." + BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers()
        );
    }

    /**
     * Get the JAAS config.
     *
     * @return The JAAS config
     */
    private static String getSaslPlainJaasConfig() {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\""
            + "user_admin=\"admin\";";
    }

    /**
     * Getter for admin client service.
     *
     * @return The admin client
     */
    public Admin getAdminClient() {
        if (adminClient == null) {
            adminClient = Admin.create(Map.of(
                "bootstrap.servers", broker.getBootstrapServers(),
                "sasl.mechanism", "PLAIN",
                "security.protocol", "SASL_PLAINTEXT",
                "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule "
                    + "required username=\"admin\" password=\"admin\";"));
        }
        return adminClient;
    }
}