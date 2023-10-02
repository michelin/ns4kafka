package com.michelin.ns4kafka.testcontainers;


import static java.lang.String.format;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * This class is a testcontainers implementation for the
 * <a href="Confluent Schema Registry">https://docs.confluent.io/current/schema-registry/index.html</a>
 * Docker container.
 */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;

    /**
     * Constructor.
     *
     * @param dockerImageName  The Docker image name of the Schema Registry
     * @param bootstrapServers The bootstrap servers URL
     */
    public SchemaRegistryContainer(DockerImageName dockerImageName, String bootstrapServers) {
        super(dockerImageName);

        this
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", bootstrapServers);

        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withNetworkAliases("schema-registry");
        waitingFor(Wait.forHttp("/subjects"));
    }

    /**
     * Get the current URL of the schema registry.
     *
     * @return The current URL of the schema registry
     */
    public String getUrl() {
        return format("http://%s:%d", this.getHost(), this.getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
    }
}

