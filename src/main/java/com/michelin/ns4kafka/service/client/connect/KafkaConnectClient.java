/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.service.client.connect;

import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.property.SecurityProperties;
import com.michelin.ns4kafka.repository.ConnectClusterRepository;
import com.michelin.ns4kafka.service.client.connect.entities.ConfigInfos;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorPluginInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorSpecs;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorStateInfo;
import com.michelin.ns4kafka.service.client.connect.entities.ConnectorStatus;
import com.michelin.ns4kafka.service.client.connect.entities.ServerInfo;
import com.michelin.ns4kafka.util.EncryptionUtils;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/** Kafka Connect client. */
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
     * @param config The Kafka Connect config
     * @return The version
     */
    public Mono<HttpResponse<ServerInfo>> version(KafkaConnectHttpConfig config) {
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/")))
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.exchange(request, ServerInfo.class));
    }

    /**
     * Get the Kafka connect version.
     *
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @return The version
     */
    public Mono<HttpResponse<ServerInfo>> version(String kafkaCluster, String connectCluster) {
        return version(getKafkaConnectConfig(kafkaCluster, connectCluster));
    }

    /**
     * List all connectors.
     *
     * @param kafkaCluster The Kafka cluster
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
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connectorClass The connector class
     * @param connectorSpecs The connector config
     * @return The configuration infos
     */
    public Mono<ConfigInfos> validate(
            String kafkaCluster, String connectCluster, String connectorClass, ConnectorSpecs connectorSpecs) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        String encodedConnectorClass = URLEncoder.encode(connectorClass, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.PUT(
                        URI.create(StringUtils.prependUri(
                                config.getUrl(), "/connector-plugins/" + encodedConnectorClass + "/config/validate")),
                        connectorSpecs)
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.retrieve(request, ConfigInfos.class));
    }

    /**
     * Create or update a connector.
     *
     * @param kafkaCluster The kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector The connector
     * @param connectorSpecs The connector config
     * @return The creation or update response
     */
    public Mono<ConnectorInfo> createOrUpdate(
            String kafkaCluster, String connectCluster, String connector, ConnectorSpecs connectorSpecs) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        String encodedConnector = URLEncoder.encode(connector, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.PUT(
                        URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + encodedConnector + "/config")),
                        connectorSpecs)
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.retrieve(request, ConnectorInfo.class));
    }

    /**
     * Delete a connector.
     *
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector The connector
     * @return The deletion response
     */
    public Mono<HttpResponse<Void>> delete(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        String encodedConnector = URLEncoder.encode(connector, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.DELETE(
                        URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + encodedConnector)))
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * List all connector plugins.
     *
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @return The list of connector plugins
     */
    public Mono<List<ConnectorPluginInfo>> connectPlugins(String kafkaCluster, String connectCluster) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);

        HttpRequest<?> request = HttpRequest.GET(
                        URI.create(StringUtils.prependUri(config.getUrl(), "/connector-plugins")))
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.retrieve(request, Argument.listOf(ConnectorPluginInfo.class)));
    }

    /**
     * Get the status of a connector.
     *
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector The connector
     * @return The status
     */
    public Mono<ConnectorStateInfo> status(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        String encodedConnector = URLEncoder.encode(connector, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.GET(
                        URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + encodedConnector + "/status")))
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.retrieve(request, ConnectorStateInfo.class));
    }

    /**
     * Restart a connector.
     *
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector The connector
     * @param taskId The task ID
     * @return The restart response
     */
    public Mono<HttpResponse<Void>> restart(String kafkaCluster, String connectCluster, String connector, int taskId) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        String encodedConnector = URLEncoder.encode(connector, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.POST(
                        URI.create(StringUtils.prependUri(
                                config.getUrl(), CONNECTORS + encodedConnector + "/tasks/" + taskId + "/restart")),
                        null)
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * Pause a connector.
     *
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector The connector
     * @return The pause response
     */
    public Mono<HttpResponse<Void>> pause(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        String encodedConnector = URLEncoder.encode(connector, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.PUT(
                        URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + encodedConnector + "/pause")),
                        null)
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * Resume a connector.
     *
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @param connector The connector
     * @return The resume response
     */
    public Mono<HttpResponse<Void>> resume(String kafkaCluster, String connectCluster, String connector) {
        KafkaConnectHttpConfig config = getKafkaConnectConfig(kafkaCluster, connectCluster);
        String encodedConnector = URLEncoder.encode(connector, StandardCharsets.UTF_8);

        HttpRequest<?> request = HttpRequest.PUT(
                        URI.create(StringUtils.prependUri(config.getUrl(), CONNECTORS + encodedConnector + "/resume")),
                        null)
                .basicAuth(config.getUsername(), config.getPassword());

        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * Get the Kafka Connect configuration.
     *
     * @param kafkaCluster The Kafka cluster
     * @param connectCluster The Kafka Connect
     * @return The Kafka Connect configuration
     */
    public KafkaConnectHttpConfig getKafkaConnectConfig(String kafkaCluster, String connectCluster) {
        Optional<ManagedClusterProperties> config = managedClusterProperties.stream()
                .filter(kafkaAsyncExecutorConfig ->
                        kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
                .findFirst();

        if (config.isEmpty()) {
            throw new ResourceValidationException(
                    null, null, List.of("Kafka cluster \"" + kafkaCluster + "\" not found"));
        }

        Optional<ConnectCluster> connectClusterOptional = connectClusterRepository.findAll().stream()
                .filter(researchConnectCluster ->
                        researchConnectCluster.getMetadata().getName().equals(connectCluster))
                .findFirst();

        if (connectClusterOptional.isPresent()) {
            return KafkaConnectHttpConfig.builder()
                    .url(connectClusterOptional.get().getSpec().getUrl())
                    .username(connectClusterOptional.get().getSpec().getUsername())
                    .password(EncryptionUtils.decryptAes256Gcm(
                            connectClusterOptional.get().getSpec().getPassword(),
                            securityProperties.getAes256EncryptionKey()))
                    .build();
        }

        ManagedClusterProperties.ConnectProperties connectConfig =
                config.get().getConnects().get(connectCluster);
        if (connectConfig == null) {
            throw new ResourceValidationException(null, null, "Connect cluster \"" + connectCluster + "\" not found");
        }

        return KafkaConnectHttpConfig.builder()
                .url(connectConfig.getUrl())
                .username(connectConfig.getBasicAuthUsername())
                .password(connectConfig.getBasicAuthPassword())
                .build();
    }

    /** Kafka Connect HTTP configuration. */
    @Getter
    @Builder
    public static class KafkaConnectHttpConfig {
        private String url;
        private String username;
        private String password;
    }
}
