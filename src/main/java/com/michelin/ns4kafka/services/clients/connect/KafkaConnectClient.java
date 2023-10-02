package com.michelin.ns4kafka.services.clients.connect;

import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.properties.SecurityProperties;
import com.michelin.ns4kafka.repositories.ConnectClusterRepository;
import com.michelin.ns4kafka.services.clients.connect.entities.ConfigInfos;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorPluginInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorStateInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorStatus;
import com.michelin.ns4kafka.services.clients.connect.entities.ServerInfo;
import com.michelin.ns4kafka.utils.EncryptionUtils;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Kafka Connect client.
 */
@Slf4j
@Singleton
public class KafkaConnectClient {
    private static final String CONNECTORS = "/connectors/";
    @Inject
    ConnectClusterRepository connectClusterRepository;
    @Inject
    @Client(id = "kafka-connect")
    private HttpClient httpClient;
    @Inject
    private List<ManagedClusterProperties> managedClusterProperties;
    @Inject
    private SecurityProperties securityProperties;

    /**
     * Get the Kafka connect version.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @return The version
     */
    public Mono<HttpResponse<ServerInfo>> version(String kafkaCluster, String connectCluster) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/")))
            .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, ServerInfo.class));
    }

    /**
     * List all connectors.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @return The connectors
     */
    public Mono<Map<String, ConnectorStatus>> listAll(String kafkaCluster, String connectCluster) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.GET(
                URI.create(StringUtils.prependUri(config.getUrl(), "/connectors?expand=info&expand=status")))
            .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, Argument.mapOf(String.class, ConnectorStatus.class)));
    }

    /**
     * Validate a connector configuration.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connectorClass The connector class
     * @param connectorSpecs The connector config
     * @return The configuration infos
     */
    public Mono<ConfigInfos> validate(String kafkaCluster, String connectCluster, String connectorClass,
                                      ConnectorSpecs connectorSpecs) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.PUT(URI.create(
                    StringUtils.prependUri(config.getUrl(), "/connector-plugins/"
                        + connectorClass + "/config/validate")),
                connectorSpecs)
            .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, ConfigInfos.class));
    }

    /**
     * Create or update a connector.
     *
     * @param kafkaCluster   The kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector      The connector
     * @param connectorSpecs The connector config
     * @return The creation or update response
     */
    public Mono<ConnectorInfo> createOrUpdate(String kafkaCluster, String connectCluster, String connector,
                                              ConnectorSpecs connectorSpecs) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request =
            HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + connector + "/config")),
                    connectorSpecs)
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, ConnectorInfo.class));
    }

    /**
     * Delete a connector.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector      The connector
     * @return The deletion response
     */
    public Mono<HttpResponse<Void>> delete(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request =
            HttpRequest.DELETE(URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + connector)))
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * List all connector plugins.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @return The list of connector plugins
     */
    public Mono<List<ConnectorPluginInfo>> connectPlugins(String kafkaCluster, String connectCluster) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request =
            HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/connector-plugins")))
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, Argument.listOf(ConnectorPluginInfo.class)));
    }

    /**
     * Get the status of a connector.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector      The connector
     * @return The status
     */
    public Mono<ConnectorStateInfo> status(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request =
            HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + connector + "/status")))
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, ConnectorStateInfo.class));
    }

    /**
     * Restart a connector.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector      The connector
     * @param taskId         The task ID
     * @return The restart response
     */
    public Mono<HttpResponse<Void>> restart(String kafkaCluster, String connectCluster, String connector, int taskId) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.POST(URI.create(
                StringUtils.prependUri(config.getUrl(), CONNECTORS + connector + "/tasks/"
                    + taskId + "/restart")), null)
            .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * Pause a connector.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector      The connector
     * @return The pause response
     */
    public Mono<HttpResponse<Void>> pause(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request =
            HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + connector + "/pause")),
                    null)
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * Resume a connector.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector      The connector
     * @return The resume response
     */
    public Mono<HttpResponse<Void>> resume(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request =
            HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + connector + "/resume")),
                    null)
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * Get the Kafka Connect configuration.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @return The Kafka Connect configuration
     */
    public KafkaConnectClient.KafkaConnectHttpConfig getKafkaConnectConfig(String kafkaCluster, String connectCluster) {
        Optional<ManagedClusterProperties> config = managedClusterProperties.stream()
            .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
            .findFirst();

        if (config.isEmpty()) {
            throw new ResourceValidationException(List.of("Kafka cluster \"" + kafkaCluster + "\" not found"), null,
                null);
        }

        Optional<ConnectCluster> connectClusterOptional = connectClusterRepository.findAll()
            .stream()
            .filter(researchConnectCluster -> researchConnectCluster.getMetadata().getName().equals(connectCluster))
            .findFirst();

        if (connectClusterOptional.isPresent()) {
            return KafkaConnectClient.KafkaConnectHttpConfig.builder()
                .url(connectClusterOptional.get().getSpec().getUrl())
                .username(connectClusterOptional.get().getSpec().getUsername())
                .password(EncryptionUtils.decryptAes256Gcm(connectClusterOptional.get().getSpec().getPassword(),
                    securityProperties.getAes256EncryptionKey()))
                .build();
        }

        ManagedClusterProperties.ConnectProperties connectConfig = config.get().getConnects().get(connectCluster);
        if (connectConfig == null) {
            throw new ResourceValidationException(List.of("Connect cluster \"" + connectCluster + "\" not found"), null,
                null);
        }

        return KafkaConnectClient.KafkaConnectHttpConfig.builder()
            .url(connectConfig.getUrl())
            .username(connectConfig.getBasicAuthUsername())
            .password(connectConfig.getBasicAuthPassword())
            .build();
    }

    /**
     * Kafka Connect HTTP configuration.
     */
    @Getter
    @Builder
    public static class KafkaConnectHttpConfig {
        private String url;
        private String username;
        private String password;
    }
}
