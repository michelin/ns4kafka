package com.michelin.ns4kafka.testcontainers;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static java.lang.String.format;

/**
 * This file is a slight adaptation of the KafkaConnectContainer code available on ydespreaux's Github account:
 *
 * @see <a href="https://github.com/ydespreaux/testcontainers/blob/master/testcontainers-kafka/src/main/java/com/github/ydespreaux/testcontainers/kafka/containers/KafkaConnectContainer.java">KafkaConnectContainer.java</a>
 */
public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

    public static final int CONNECT_REST_PORT_INTERNAL = 8083;
    /**
     * Key / value for Configuration
     */
    public static final String GROUP_ID_CONFIG = "CONNECT_GROUP_ID";
    public static final String OFFSET_STORAGE_FILE_FILENAME_CONFIG = "CONNECT_OFFSET_STORAGE_FILE_FILENAME";
    public static final String OFFSET_STORAGE_TOPIC_CONFIG = "CONNECT_OFFSET_STORAGE_TOPIC";
    public static final String OFFSET_STORAGE_PARTITIONS_CONFIG = "CONNECT_OFFSET_STORAGE_PARTITIONS";
    public static final String CONFIG_STORAGE_TOPIC_CONFIG = "CONNECT_CONFIG_STORAGE_TOPIC";
    public static final String STATUS_STORAGE_TOPIC_CONFIG = "CONNECT_STATUS_STORAGE_TOPIC";
    public static final String STATUS_STORAGE_PARTITIONS_CONFIG = "CONNECT_STATUS_STORAGE_PARTITIONS";
    public static final String KEY_CONVERTER_CONFIG = "CONNECT_KEY_CONVERTER";
    public static final String KEY_CONVERTER_SCHEMA_REGISTRY_URL_CONFIG = "CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL";
    public static final String VALUE_CONVERTER_CONFIG = "CONNECT_VALUE_CONVERTER";
    public static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL_CONFIG = "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL";
    private static final String AVRO_CONVERTER_PATTERN = "AvroConverter";
    private static final String PLUGIN_PATH_CONTAINER = "/usr/share/java";
    private static final String GROUP_ID_DEFAULT_VALUE = "kafka-connect-group";
    private static final String OFFSET_STORAGE_FILE_FILENAME_DEFAULT_VALUE = "connect-offsets-file.txt";
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
    private static final String KEY_CONVERTER_SCHEMAS_ENABLE_CONFIG = "CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE";
    private static final String VALUE_CONVERTER_DEFAULT_VALUE = "org.apache.kafka.connect.json.JsonConverter";
    private static final String VALUE_CONVERTER_SCHEMAS_ENABLE_CONFIG = "CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE";
    private static final String INTERNAL_KEY_CONVERTER_CONFIG = "CONNECT_INTERNAL_KEY_CONVERTER";
    private static final String INTERNAL_KEY_CONVERTER_DEFAULT_VALUE = "org.apache.kafka.connect.json.JsonConverter";
    private static final String INTERNAL_VALUE_CONVERTER_CONFIG = "CONNECT_INTERNAL_VALUE_CONVERTER";
    private static final String INTERNAL_VALUE_CONVERTER_DEFAULT_VALUE = "org.apache.kafka.connect.json.JsonConverter";


    private final String bootstrapServers;
    private final String networkAlias = "kafka-connect";
    /**
     * Schema registry url
     */
    private String schemaRegistryUrl;
    private boolean hasKeyAvroConverter = false;
    private boolean hasValueAvroConverter = false;


    public KafkaConnectContainer(DockerImageName dockerImageName, String bootstrapServers) {
        super(dockerImageName);
        this.bootstrapServers = bootstrapServers;
        //this.withLogConsumer(containerLogsConsumer(logger()))
        this.withEnv(GROUP_ID_CONFIG, GROUP_ID_DEFAULT_VALUE)
            .withEnv(KEY_CONVERTER_CONFIG, KEY_CONVERTER_DEFAULT_VALUE)
            .withEnv(VALUE_CONVERTER_CONFIG, VALUE_CONVERTER_DEFAULT_VALUE)
            //.withEnv(OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORAGE_FILE_FILENAME_DEFAULT_VALUE)
            .withEnv(OFFSET_STORAGE_TOPIC_CONFIG, OFFSET_STORAGE_TOPIC_DEFAULT_VALUE)
            .withEnv(OFFSET_STORAGE_PARTITIONS_CONFIG, String.valueOf(OFFSET_STORAGE_PARTITIONS_DEFAULT_VALUE))
            .withEnv(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, String.valueOf(OFFSET_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE))
            .withEnv(CONFIG_STORAGE_TOPIC_CONFIG, CONFIG_STORAGE_TOPIC_DEFAULT_VALUE)
            .withEnv(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, String.valueOf(CONFIG_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE))
            .withEnv(STATUS_STORAGE_TOPIC_CONFIG, STATUS_STORAGE_TOPIC_DEFAULT_VALUE)
            .withEnv(STATUS_STORAGE_PARTITIONS_CONFIG, String.valueOf(STATUS_STORAGE_PARTITIONS_DEFAULT_VALUE))
            .withEnv(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, String.valueOf(STATUS_STORAGE_REPLICATION_FACTOR_DEFAULT_VALUE))
            .withEnv(INTERNAL_KEY_CONVERTER_CONFIG, INTERNAL_KEY_CONVERTER_DEFAULT_VALUE)
            .withEnv(INTERNAL_VALUE_CONVERTER_CONFIG, INTERNAL_VALUE_CONVERTER_DEFAULT_VALUE);

        this.withPlugins("connectors/confluentinc-kafka-connect-datagen-0.5.2");

        withExposedPorts(CONNECT_REST_PORT_INTERNAL);
        withNetworkAliases(networkAlias);

        waitingFor(Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(120L)));
    }

    /**
     * Configure the container
     */
    @Override
    protected void configure() {
        super.configure();
        this.withEnv("CONNECT_BOOTSTRAP_SERVERS", this.bootstrapServers)
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "kafka-connect")
                .withEnv("CONNECT_PLUGIN_PATH", PLUGIN_PATH_CONTAINER)
                .withEnv("CONNECT_LOG4J_LOGGERS", "org.reflections=ERROR")
                .withEnv("CONNECT_REST_PORT", String.valueOf(CONNECT_REST_PORT_INTERNAL))
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withName("testcontainsers-kafka-connect-" + UUID.randomUUID()));


        if (hasKeyAvroConverter) {
            Objects.requireNonNull(this.schemaRegistryUrl, "Schema registry URL not defined !!");
            this.withEnv(KEY_CONVERTER_SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
            this.withEnv(KEY_CONVERTER_SCHEMAS_ENABLE_CONFIG, "true");
        }
        if (hasValueAvroConverter) {
            Objects.requireNonNull(this.schemaRegistryUrl, "Schema registry URL not defined !!");
            this.withEnv(VALUE_CONVERTER_SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
            this.withEnv(VALUE_CONVERTER_SCHEMAS_ENABLE_CONFIG, "true");
        }
    }

    public KafkaConnectContainer withSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        return this;
    }

    /**
     * Set the group id
     *
     * @param groupId
     * @return
     */
    public KafkaConnectContainer withGroupId(String groupId) {
        if (groupId != null) {
            withEnv(GROUP_ID_CONFIG, groupId);
        }
        return this;
    }

    /**
     * Set the config storage topic.
     *
     * @param topic
     * @return
     */
    public KafkaConnectContainer withConfigStorageTopic(String topic) {
        if (topic != null) {
            withEnv(CONFIG_STORAGE_TOPIC_CONFIG, topic);
        }
        return this;
    }

    /**
     * Set the topic name of the storage offsets topic.
     *
     * @param topic
     * @return
     */
    public KafkaConnectContainer withOffsetStorageTopic(String topic) {
        if (topic != null) {
            withEnv(OFFSET_STORAGE_TOPIC_CONFIG, topic);
        }
        return this;
    }

    /**
     * Set the offsets storage partition.
     *
     * @param partitions
     * @return
     */
    public KafkaConnectContainer withOffsetStoragePartition(Integer partitions) {
        if (partitions != null) {
            withEnv(OFFSET_STORAGE_PARTITIONS_CONFIG, String.valueOf(partitions));
        }
        return this;
    }

    /**
     * Set the status storage topic's name.
     *
     * @param topic
     * @return
     */
    public KafkaConnectContainer withStatusStorageTopic(String topic) {
        if (topic != null) {
            withEnv(STATUS_STORAGE_TOPIC_CONFIG, topic);
        }
        return this;
    }

    /**
     * Set the status storage partition.
     *
     * @param partitions
     * @return
     */
    public KafkaConnectContainer withStatusStoragePartition(Integer partitions) {
        if (partitions != null) {
            withEnv(STATUS_STORAGE_PARTITIONS_CONFIG, String.valueOf(partitions));
        }
        return this;
    }

    /**
     * Set the offsets storage file name.
     *
     * @param storageFilename
     * @return
     */
    public KafkaConnectContainer withOffsetStorageFilename(String storageFilename) {
        if (storageFilename != null) {
            withEnv(OFFSET_STORAGE_FILE_FILENAME_CONFIG, storageFilename);
        }
        return this;
    }

    /**
     * Set the key converter.
     *
     * @param keyConverter
     * @return
     */
    public KafkaConnectContainer withKeyConverter(String keyConverter) {
        if (keyConverter != null) {
            withEnv(KEY_CONVERTER_CONFIG, keyConverter);
            this.hasKeyAvroConverter = keyConverter.contains(AVRO_CONVERTER_PATTERN);
        }
        return this;
    }

    /**
     * Set the value converter.
     *
     * @param valueConverter
     * @return
     */
    public KafkaConnectContainer withValueConverter(String valueConverter) {
        if (valueConverter != null) {
            withEnv(VALUE_CONVERTER_CONFIG, valueConverter);
            this.hasValueAvroConverter = valueConverter.contains(AVRO_CONVERTER_PATTERN);
        }
        return this;
    }

    /**
     * Set the list of plugins directory.
     *
     * @param plugins
     * @return
     */
    public KafkaConnectContainer withPlugins(Set<String> plugins) {
        if (plugins == null) {
            return this;
        }
        plugins.forEach(this::withPlugins);
        return this;
    }

    /**
     * Set the plugins directory.
     *
     * @param plugins
     * @return
     */
    public KafkaConnectContainer withPlugins(String plugins) {
        if (plugins == null) {
            return this;
        }
        MountableFile mountableFile = MountableFile.forClasspathResource(plugins);
        Path pluginsPath = Paths.get(mountableFile.getResolvedPath());
        File pluginsFile = pluginsPath.toFile();
        if (!pluginsFile.exists()) {
            throw new IllegalArgumentException(format("Resource with path %s could not be found", pluginsPath.toString()));
        }
        String containerPath = PLUGIN_PATH_CONTAINER;
        if (pluginsFile.isDirectory()) {
            containerPath += "/" + pluginsPath.getFileName();
        } else {
            containerPath += "/" + pluginsPath.getParent().getFileName() + "/" + pluginsPath.getFileName();
        }
        // Create the volume that will be need for scripts
        this.addFileSystemBind(mountableFile.getResolvedPath(), containerPath, BindMode.READ_ONLY);
        return this;
    }

    /**
     * Get the url.
     *
     * @return
     */
    public String getUrl() {
        return format("http://%s:%d", this.getContainerIpAddress(), getMappedPort(CONNECT_REST_PORT_INTERNAL));
    }

    /**
     * Get the local url
     *
     * @return
     */
    public String getInternalUrl() {
        return format("http://%s:%d", this.getNetworkAliases().get(0), CONNECT_REST_PORT_INTERNAL);
    }
}
