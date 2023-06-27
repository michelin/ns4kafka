package com.michelin.ns4kafka.services.clients.schema;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaRequest;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaResponse;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
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

    public Mono<List<String>> getSubjects(String kafkaCluster) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects")))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, Argument.listOf(String.class)));
    }

    // Maybe
    public Mono<SchemaResponse> getLatestSubject(String kafkaCluster, String subject) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects/" + subject + "/versions/latest")))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaResponse.class));
    }

    // Maybe
    public Mono<SchemaResponse> register(String kafkaCluster, String subject, SchemaRequest body) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.POST(URI.create(StringUtils.prependUri(config.getUrl(), "/subjects/" + subject + "/versions")), body)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaResponse.class));
    }

    // Maybe
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
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityCheckResponse.class));
    }

    public Mono<SchemaCompatibilityResponse> updateSubjectCompatibility(String kafkaCluster, String subject, String compatibility) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.PUT(URI.create(StringUtils.prependUri(config.getUrl(), "/config/" + subject)), compatibility)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class));
    }

    // Maybe
    public Mono<SchemaCompatibilityResponse> getCurrentCompatibilityBySubject(String kafkaCluster, String subject) {
        KafkaAsyncExecutorConfig.RegistryConfig config = getSchemaRegistry(kafkaCluster);
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(config.getUrl(), "/config/" + subject)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
        return Mono.from(httpClient.retrieve(request, SchemaCompatibilityResponse.class));
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
