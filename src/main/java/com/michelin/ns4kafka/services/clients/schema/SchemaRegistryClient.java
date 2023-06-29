package com.michelin.ns4kafka.services.clients.schema;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.clients.schema.entities.*;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Optional;

@Slf4j
@Singleton
public class SchemaRegistryClient {
    @Inject
    @Client(id = "schema-registry")
    private HttpClient httpClient;

    @Inject
    private List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    /**
     * List subjects
     * @param kafkaCluster The Kafka cluster
     * @return A list of subjects
     */
    public Mono<List<String>> getSubjects(String kafkaCluster) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects")))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, Argument.listOf(String.class)));
    }

    /**
     * Get a latest version of a subject
     * @param kafkaCluster The Kafka cluster
     * @param subject The subject
     * @return A version of a subject
     */
    public Mono<SchemaResponse> getLatestSubject(String kafkaCluster, String subject) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects/" + subject + "/versions/latest")))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaResponse.class))
                .onErrorResume(HttpClientResponseException.class,
                        ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Mono.empty() : Mono.error(ex));
    }

    /**
     * Register a subject and a schema
     * @param kafkaCluster The Kafka cluster
     * @param subject The subject
     * @param body The schema
     * @return The response of the registration
     */
    public Mono<SchemaResponse> register(String kafkaCluster, String subject, SchemaRequest body) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.POST(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects/" + subject + "/versions")), body)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaResponse.class));
    }

    /**
     * Delete a subject
     * @param kafkaCluster The Kafka cluster
     * @param subject The subject
     * @param hardDelete Should the subject be hard deleted or not
     * @return The versions of the deleted subject
     */
    public Mono<Integer[]> deleteSubject(String kafkaCluster, String subject, boolean hardDelete) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        MutableHttpRequest<?> request = HttpRequest.DELETE(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects/" + subject + "?permanent=" + hardDelete)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, Integer[].class));
    }

    public Mono<SchemaCompatibilityCheckResponse> validateSchemaCompatibility(String kafkaCluster, String subject, SchemaRequest body) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.POST(URI.create(StringUtils.prependUri(config.getUrl(), "/compatibility/subjects/" + subject + "/versions?verbose=true")), body)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityCheckResponse.class))
                .onErrorResume(HttpClientResponseException.class,
                        ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Mono.empty() : Mono.error(ex));
    }

    public Mono<SchemaCompatibilityResponse> updateSubjectCompatibility(String kafkaCluster, String subject, SchemaCompatibilityRequest body) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), "/config/" + subject)), body)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class));
    }

    public Mono<SchemaCompatibilityResponse> getCurrentCompatibilityBySubject(String kafkaCluster, String subject) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/config/" + subject)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class))
                .onErrorResume(HttpClientResponseException.class,
                        ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Mono.empty() : Mono.error(ex));
    }

    public Mono<SchemaCompatibilityResponse> deleteCurrentCompatibilityBySubject(String kafkaCluster, String subject) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        MutableHttpRequest<?> request = HttpRequest.DELETE(URI.create(StringUtils.prependUri(config.getUrl(), "/config/" + subject)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class));
    }

    private KafkaAsyncExecutorConfig.RegistryConfig getSchemaRegistry(String kafkaCluster) {
        Optional<KafkaAsyncExecutorConfig> config = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
                .findFirst();

        if (config.isEmpty()) {
            throw new ResourceValidationException(List.of("Kafka Cluster [" + kafkaCluster + "] not found"), null, null);
        }

        if (config.get().getSchemaRegistry() == null) {
            throw new ResourceValidationException(List.of("Kafka Cluster [" + kafkaCluster + "] has no schema registry"), null, null);
        }

        return config.get().getSchemaRegistry();
    }
}
