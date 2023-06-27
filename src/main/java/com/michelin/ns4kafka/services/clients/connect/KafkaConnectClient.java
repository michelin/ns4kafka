package com.michelin.ns4kafka.services.clients.connect;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.repositories.ConnectClusterRepository;
import com.michelin.ns4kafka.services.ConnectClusterService;
import com.michelin.ns4kafka.services.clients.connect.entities.*;
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
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Singleton
public class KafkaConnectClient {
    @Inject
    @Client(id = "kafka-connect")
    private HttpClient httpClient;

    @Inject
    private List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Inject
    ConnectClusterRepository connectClusterRepository;

    @Inject
    private SecurityConfig securityConfig;

    public HttpResponse<ServerInfo> version(String kafkaCluster, String connectCluster) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/")))
                .basicAuth(config.getUsername(), config.getPassword());
        return httpClient.toBlocking().exchange(request, ServerInfo.class);
    }

    public Mono<Map<String, ConnectorStatus>> listAll(String kafkaCluster, String connectCluster) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/connectors?expand=info&expand=status")))
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, Argument.mapOf(String.class, ConnectorStatus.class)));
    }

    public Mono<ConfigInfos> validate(String kafkaCluster, String connectCluster, String connectorClass, ConnectorSpecs connectorSpecs) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), "/connector-plugins/" + connectorClass + "/config/validate")), connectorSpecs)
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, ConfigInfos.class));
    }

    public Mono<ConnectorInfo> createOrUpdate(String kafkaCluster, String connectCluster, String connector, ConnectorSpecs connectorSpecs) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), "/connectors/" + connector + "/config")), connectorSpecs)
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, ConnectorInfo.class));
    }

    public Mono<HttpResponse<Void>> delete(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.DELETE(URI.create(StringUtils.prependUri(config.getUrl(), "/connectors/" + connector)))
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, Void.class));
    }

    public Mono<List<ConnectorPluginInfo>> connectPlugins(String kafkaCluster, String connectCluster) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/connector-plugins")))
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, Argument.listOf(ConnectorPluginInfo.class)));
    }

    public Mono<ConnectorStateInfo> status(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/connectors/" + connector + "/status")))
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.retrieve(request, ConnectorStateInfo.class));
    }

    public Mono<HttpResponse<Void>> restart(String kafkaCluster, String connectCluster, String connector, int taskId) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.POST(URI.create(StringUtils.prependUri(config.getUrl(), "/connectors/" + connector + "/tasks/" + taskId + "/restart")), null)
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, Void.class));
    }

    public Mono<HttpResponse<Void>> pause(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), "/connectors/" + connector + "/pause")), null)
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, Void.class));
    }

    public Mono<HttpResponse<Void>> resume(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        HttpRequest<?> request = HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), "/connectors/" + connector + "/resume")), null)
                .basicAuth(config.getUsername(), config.getPassword());
        return Mono.from(httpClient.exchange(request, Void.class));
    }

    public KafkaConnectClient.KafkaConnectHttpConfig getKafkaConnectConfig(String kafkaCluster, String connectCluster) {
        Optional<KafkaAsyncExecutorConfig> config = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
                .findFirst();

        if (config.isEmpty()) {
            throw new ResourceValidationException(List.of("Kafka cluster \"" + kafkaCluster + "\" not found"), null, null);
        }

        Optional<ConnectCluster> connectClusterOptional = connectClusterRepository.findAll()
                .stream()
                .filter(researchConnectCluster -> researchConnectCluster.getMetadata().getName().equals(connectCluster))
                .findFirst();

        if (connectClusterOptional.isPresent()) {
            log.debug("Self deployed Connect cluster {} found in namespace {}", connectCluster, connectClusterOptional.get().getMetadata().getNamespace());
            return KafkaConnectClient.KafkaConnectHttpConfig.builder()
                    .url(connectClusterOptional.get().getSpec().getUrl())
                    .username(connectClusterOptional.get().getSpec().getUsername())
                    .password(EncryptionUtils.decryptAES256GCM(connectClusterOptional.get().getSpec().getPassword(), securityConfig.getAes256EncryptionKey()))
                    .build();
        }

        KafkaAsyncExecutorConfig.ConnectConfig connectConfig = config.get().getConnects().get(connectCluster);
        if (connectConfig == null) {
            throw new ResourceValidationException(List.of("Connect cluster \"" + connectCluster + "\" not found"), null, null);
        }

        log.debug("Connect cluster {} found in Ns4Kafka configuration", connectCluster);

        return KafkaConnectClient.KafkaConnectHttpConfig.builder()
                .url(connectConfig.getUrl())
                .username(connectConfig.getBasicAuthUsername())
                .password(connectConfig.getBasicAuthPassword())
                .build();
    }

    @Builder
    @Getter
    public static class KafkaConnectHttpConfig {
        private String url;
        private String username;
        private String password;
    }
}
