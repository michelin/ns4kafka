package com.michelin.ns4kafka.testcontainers;

import static java.lang.String.format;

import java.time.Duration;
import java.util.UUID;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * This file is a slight adaptation of the KafkaConnectContainer code.
 * Available on ydespreaux's GitHub account.
 *
 * @see <a href="https://github.com/ydespreaux/testcontainers/blob/master/testcontainers-kafka/src/main/java/com/github/ydespreaux/testcontainers/kafka/containers/KafkaConnectContainer.java">KafkaConnectContainer.java</a>
 */
public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {
    public static final int CONNECT_REST_PORT_INTERNAL = 8083;
    public static final String GROUP_ID_CONFIG = "CONNECT_GROUP_ID";
    public static final String OFFSET_STORAGE_TOPIC_CONFIG = "CONNECT_OFFSET_STORAGE_TOPIC";
    public static final String OFFSET_STORAGE_PARTITIONS_CONFIG = "CONNECT_OFFSET_STORAGE_PARTITIONS";
    public static final String CONFIG_STORAGE_TOPIC_CONFIG = "CONNECT_CONFIG_STORAGE_TOPIC";
    public static final String STATUS_STORAGE_TOPIC_CONFIG = "CONNECT_STATUS_STORAGE_TOPIC";
    public static final String STATUS_STORAGE_PARTITIONS_CONFIG = "CONNECT_STATUS_STORAGE_PARTITIONS";
    public static final String KEY_CONVERTER_CONFIG = "CONNECT_KEY_CONVERTER";
    public static final String VALUE_CONVERTER_CONFIG = "CONNECT_VALUE_CONVERTER";
    private static final String PLUGIN_PATH_CONTAINER = "/usr/share/java,/usr/share/filestream-connectors";
    private static final String GROUP_ID_DEFAULT_VALUE = "kafka-connect-group";
    private static final String OFFSET_STORAGE_TOPIC_DEFAULT_VALUE = "connect-offsets";
    private static final Integer OFFSET_STORAGE_PARTITIONS_DEFAULT_VALUE = 3;
    private static final String OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG = "CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR";
    private static final Integer OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE = 1;
    private static final String CONFIG_STORAGE_TOPIC_DEFAULT_VALUE = "connect-config";
    private static final String CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG = "CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR";
    private static final Integer CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE = 1;
    private static final String STATUS_STORAGE_TOPIC_DEFAULT_VALUE = "connect-status";
    private static final Integer STATUS_STORAGE_PARTITIONS_DEFAULT_VALUE = 3;
    private static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG = "CONNECT_STATUS_STORAGE_REPLICATION_FACTOR";
    private static final Integer STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE = 1;
    private static final String KEY_CONVERTER_DEFAULT_VALUE = "org.apache.kafka.connect.json.JsonConverter";
    private static final String VALUE_CONVERTER_DEFAULT_VALUE = "org.apache.kafka.connect.json.JsonConverter";
    private static final String INTERNAL_KEY_CONVERTER_CONFIG = "CONNECT_INTERNAL_KEY_CONVERTER";
    private static final String INTERNAL_KEY_CONVERTER_DEFAULT_VALUE = "org.apache.kafka.connect.json.JsonConverter";
    private static final String INTERNAL_VALUE_CONVERTER_CONFIG = "CONNECT_INTERNAL_VALUE_CONVERTER";
    private static final String INTERNAL_VALUE_CONVERTER_DEFAULT_VALUE = "org.apache.kafka.connect.json.JsonConverter";
    private final String bootstrapServers;

    /**
     * Constructor.
     *
     * @param dockerImageName  The docker image name
     * @param bootstrapServers The bootstrap servers
     */
    public KafkaConnectContainer(DockerImageName dockerImageName, String bootstrapServers) {
        super(dockerImageName);
        this.bootstrapServers = bootstrapServers;
        this.withEnv(GROUP_ID_CONFIG, GROUP_ID_DEFAULT_VALUE)
            .withEnv(KEY_CONVERTER_CONFIG, KEY_CONVERTER_DEFAULT_VALUE)
            .withEnv(VALUE_CONVERTER_CONFIG, VALUE_CONVERTER_DEFAULT_VALUE)
            .withEnv(OFFSET_STORAGE_TOPIC_CONFIG, OFFSET_STORAGE_TOPIC_DEFAULT_VALUE)
            .withEnv(OFFSET_STORAGE_PARTITIONS_CONFIG, String.valueOf(OFFSET_STORAGE_PARTITIONS_DEFAULT_VALUE))
            .withEnv(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG,
                String.valueOf(OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE))
            .withEnv(CONFIG_STORAGE_TOPIC_CONFIG, CONFIG_STORAGE_TOPIC_DEFAULT_VALUE)
            .withEnv(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG,
                String.valueOf(CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE))
            .withEnv(STATUS_STORAGE_TOPIC_CONFIG, STATUS_STORAGE_TOPIC_DEFAULT_VALUE)
            .withEnv(STATUS_STORAGE_PARTITIONS_CONFIG, String.valueOf(STATUS_STORAGE_PARTITIONS_DEFAULT_VALUE))
            .withEnv(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG,
                String.valueOf(STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE))
            .withEnv(INTERNAL_KEY_CONVERTER_CONFIG, INTERNAL_KEY_CONVERTER_DEFAULT_VALUE)
            .withEnv(INTERNAL_VALUE_CONVERTER_CONFIG, INTERNAL_VALUE_CONVERTER_DEFAULT_VALUE);

        withExposedPorts(CONNECT_REST_PORT_INTERNAL);
        withNetworkAliases("kafka-connect");
        waitingFor(Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(120L)));
    }

    /**
     * Configure the container.
     */
    @Override
    protected void configure() {
        super.configure();
        this.withEnv("CONNECT_BOOTSTRAP_SERVERS", this.bootstrapServers)
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "kafka-connect")
            .withEnv("CONNECT_PLUGIN_PATH", PLUGIN_PATH_CONTAINER)
            .withEnv("CONNECT_LOG4J_LOGGERS", "org.reflections=ERROR")
            .withEnv("CONNECT_REST_PORT", String.valueOf(CONNECT_REST_PORT_INTERNAL))
            .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName(
                "testcontainsers-kafka-connect-" + UUID.randomUUID()));
    }

    /**
     * Get the url of Kafka Connect.
     *
     * @return The URL
     */
    public String getUrl() {
        return format("http://%s:%d", this.getHost(), getMappedPort(CONNECT_REST_PORT_INTERNAL));
    }
}
