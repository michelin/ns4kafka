package com.michelin.ns4kafka.services.schema;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.OncePerRequestHttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Filter(KafkaSchemaRegistryClientProxy.SCHEMA_REGISTRY_PREFIX + "/**")
public class KafkaSchemaRegistryClientProxy extends OncePerRequestHttpServerFilter {
    /**
     * Schema registry prefix used to filter request to schema registries. It'll be replace by
     * the schema registry URL of the
     */
    public static final String SCHEMA_REGISTRY_PREFIX = "/schema-registry-proxy";

    /**
     * A header that contains the Kafka cluster
     */
    public static final String PROXY_HEADER_KAFKA_CLUSTER = "X-Kafka-Cluster";

    /**
     * A header that contains a secret for the request
     */
    public static final String PROXY_HEADER_SECRET = "X-Proxy-Secret";

    /**
     * Generate a secret
     */
    public static final String PROXY_SECRET = UUID.randomUUID().toString();

    /**
     * Managed clusters configuration
     */
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    /**
     * HTTP client
     */
    @Inject
    ProxyHttpClient client;

    /**
     * Filter requests
     *
     * @param request The request to filter
     * @param chain The servlet chain
     * @return A modified request
     */
    @Override
    public Publisher<MutableHttpResponse<?>> doFilterOnce(HttpRequest<?> request, ServerFilterChain chain) {
        // Check call is initiated from Micronaut and not from outside
        if (!request.getHeaders().contains(KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET)) {
            return Publishers.just(new ResourceValidationException(List.of("Missing required header " + KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET), null, null));
        }

        String secret = request.getHeaders().get(KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET);
        if (!PROXY_SECRET.equals(secret)) {
            return Publishers.just(new ResourceValidationException(List.of("Invalid value " + secret + " for header " + KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET), null, null));
        }

        // Retrieve the connectConfig based on Header
        if (!request.getHeaders().contains(KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER)) {
            return Publishers.just(new ResourceValidationException(List.of("Missing required header " + KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER), null, null));
        }

        String kafkaCluster = request.getHeaders().get(KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER);

        Optional<KafkaAsyncExecutorConfig> config = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
                .findFirst();

        if (config.isEmpty()) {
            return Publishers.just(new ResourceValidationException(List.of("Kafka Cluster [" + kafkaCluster + "] not found"),null,null));
        }

        if (config.get().getSchemaRegistry() == null) {
            return Publishers.just(new ResourceValidationException(List.of("Kafka Cluster [" + kafkaCluster + "] has no schema registry"),null,null));
        }

        return this.client.proxy(mutateSchemaRegistryRequest(request, config.get()));
    }

    /**
     * Mutate a request to the Schema Registry by modifying the base URI by the Schema Registry URI from the
     * cluster config
     *
     * @param request The request to modify
     * @param config The configuration used to modify the request
     * @return The modified request
     */
    public MutableHttpRequest<?> mutateSchemaRegistryRequest(HttpRequest<?> request, KafkaAsyncExecutorConfig config) {
        URI newURI = URI.create(config.getSchemaRegistry().getUrl());

        MutableHttpRequest<?> mutableHttpRequest = request.mutate()
                .uri(mutableRequest -> mutableRequest
                        .scheme(newURI.getScheme())
                        .host(newURI.getHost())
                        .port(newURI.getPort())
                        .replacePath(StringUtils.prependUri(newURI.getPath(),
                                request.getPath().substring(KafkaSchemaRegistryClientProxy.SCHEMA_REGISTRY_PREFIX.length())
                        ))
                );

        if (StringUtils.isNotEmpty(config.getSchemaRegistry().getBasicAuthUsername()) &&
                StringUtils.isNotEmpty(config.getSchemaRegistry().getBasicAuthPassword())) {
            mutableHttpRequest.basicAuth(config.getSchemaRegistry().getBasicAuthUsername(),
                    config.getSchemaRegistry().getBasicAuthPassword());
        }

        mutableHttpRequest.getHeaders().remove(HttpHeaders.HOST);
        return mutableHttpRequest;
    }
}
