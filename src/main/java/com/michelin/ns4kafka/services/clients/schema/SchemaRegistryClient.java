package com.michelin.ns4kafka.services.clients.schema;

import com.michelin.ns4kafka.properties.KafkaAsyncExecutorProperties;
import com.michelin.ns4kafka.services.clients.schema.entities.*;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Optional;

@Slf4j
@Singleton
public class SchemaRegistryClient {
    private static final String SUBJECTS = "/subjects/";
    private static final String CONFIG = "/config/";

    @Inject
    @Client(id = "schema-registry")
    private HttpClient httpClient;

    @Inject
    private List<KafkaAsyncExecutorProperties> kafkaAsyncExecutorProperties;

    /**
     * List subjects
     * @param kafkaCluster The Kafka cluster
     * @return A list of subjects
     */
    public Flux<String> getSubjects(String kafkaCluster) {
        KafkaAsyncExecutorProperties.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects")))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Flux.from(httpClient.retrieve(request, String[].class)).flatMap(Flux::fromArray);
    }

    /**
     * Get a latest version of a subject
     * @param kafkaCluster The Kafka cluster
     * @param subject The subject
     * @return A version of a subject
     */
    public Mono<SchemaResponse> getLatestSubject(String kafkaCluster, String subject) {
        KafkaAsyncExecutorProperties.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), SUBJECTS + subject + "/versions/latest")))
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
        KafkaAsyncExecutorProperties.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.POST(URI.create(StringUtils.prependUri(config.getUrl(), SUBJECTS + subject + "/versions")), body)
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
        KafkaAsyncExecutorProperties.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        MutableHttpRequest<?> request = HttpRequest.DELETE(URI.create(StringUtils.prependUri(config.getUrl(), SUBJECTS + subject + "?permanent=" + hardDelete)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, Integer[].class));
    }

    /**
     * Validate the schema compatibility
     * @param kafkaCluster The Kafka cluster
     * @param subject The subject
     * @param body The request
     * @return The schema compatibility validation
     */
    public Mono<SchemaCompatibilityCheckResponse> validateSchemaCompatibility(String kafkaCluster, String subject, SchemaRequest body) {
        KafkaAsyncExecutorProperties.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.POST(URI.create(StringUtils.prependUri(config.getUrl(), "/compatibility/subjects/" + subject + "/versions?verbose=true")), body)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityCheckResponse.class))
                .onErrorResume(HttpClientResponseException.class,
                        ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Mono.empty() : Mono.error(ex));
    }

    /**
     * Update the subject compatibility
     * @param kafkaCluster The Kafka cluster
     * @param subject The subject
     * @param body The schema compatibility request
     * @return The schema compatibility update
     */
    public Mono<SchemaCompatibilityResponse> updateSubjectCompatibility(String kafkaCluster, String subject, SchemaCompatibilityRequest body) {
        KafkaAsyncExecutorProperties.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), CONFIG + subject)), body)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class));
    }

    /**
     * Get the current compatibility by subject
     * @param kafkaCluster The Kafka cluster
     * @param subject The subject
     * @return The current schema compatibility
     */
    public Mono<SchemaCompatibilityResponse> getCurrentCompatibilityBySubject(String kafkaCluster, String subject) {
        KafkaAsyncExecutorProperties.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), CONFIG + subject)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class))
                .onErrorResume(HttpClientResponseException.class,
                        ex -> ex.getStatus().equals(HttpStatus.NOT_FOUND) ? Mono.empty() : Mono.error(ex));
    }

    /**
     * Delete current compatibility by subject
     * @param kafkaCluster The Kafka cluster
     * @param subject The subject
     * @return The deleted schema compatibility
     */
    public Mono<SchemaCompatibilityResponse> deleteCurrentCompatibilityBySubject(String kafkaCluster, String subject) {
        KafkaAsyncExecutorProperties.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        MutableHttpRequest<?> request = HttpRequest.DELETE(URI.create(StringUtils.prependUri(config.getUrl(), CONFIG + subject)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class));
    }

    /**
     * Get the schema registry of the given Kafka cluster
     * @param kafkaCluster The Kafka cluster
     * @return The schema registry configuration
     */
    private KafkaAsyncExecutorProperties.RegistryConfig getSchemaRegistry(String kafkaCluster) {
        Optional<KafkaAsyncExecutorProperties> config = kafkaAsyncExecutorProperties.stream()
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
