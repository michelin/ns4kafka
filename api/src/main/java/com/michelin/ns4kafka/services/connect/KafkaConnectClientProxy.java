package com.michelin.ns4kafka.services.connect;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig.ConnectConfig;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import jakarta.inject.Inject;
import org.reactivestreams.Publisher;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Filter(KafkaConnectClientProxy.PROXY_PREFIX + "/**")
public class KafkaConnectClientProxy implements HttpServerFilter {
    public static final String PROXY_PREFIX = "/connect-proxy";
    public static final String PROXY_HEADER_KAFKA_CLUSTER = "X-Kafka-Cluster";
    public static final String PROXY_HEADER_CONNECT_CLUSTER = "X-Connect-Cluster";

    // This UUID prevents anyone to access this filter directly and bypassing ConnectController and ConnectService.
    // Only Micronaut can call this filter successfully
    public static final String PROXY_HEADER_SECRET = "X-Proxy-Secret";
    public static final String PROXY_SECRET = UUID.randomUUID().toString();

    @Inject
    ProxyHttpClient client;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        // check call is initiated from micronaut and not from outisde
        if (!request.getHeaders().contains(KafkaConnectClientProxy.PROXY_HEADER_SECRET)) {
            return Publishers.just(new ResourceValidationException(List.of("Missing required Header " + KafkaConnectClientProxy.PROXY_HEADER_SECRET), null, null));
        }
        String secret = request.getHeaders().get(KafkaConnectClientProxy.PROXY_HEADER_SECRET);
        if (!PROXY_SECRET.equals(secret)) {
            return Publishers.just(new ResourceValidationException(List.of("Invalid value " + secret + " for Header " + KafkaConnectClientProxy.PROXY_HEADER_SECRET), null, null));
        }
        // retrieve the connectConfig based on Header
        if (!request.getHeaders().contains(KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER)) {
            return Publishers.just(new ResourceValidationException(List.of("Missing required Header " + KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER), null, null));
        }
        if (!request.getHeaders().contains(KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER)) {
            return Publishers.just(new ResourceValidationException(List.of("Missing required Header " + KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER), null, null));
        }

        String kafkaCluster = request.getHeaders().get(KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER);
        String connectCluster = request.getHeaders().get(KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER);

        // get config of the kafkaCluster
        Optional<KafkaAsyncExecutorConfig> config = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
                .findFirst();
        if (config.isEmpty()) {
            return Publishers.just(new ResourceValidationException(List.of("Kafka Cluster [" + kafkaCluster + "] not found"),null,null));
        }

        // get the good connect config
        ConnectConfig connectConfig = config.get().getConnects().get(connectCluster);
        if (connectConfig == null) {
            return Publishers.just(new ResourceValidationException(List.of("Connect Cluster [" + connectCluster + "] not found"), null, null));
        }

        // mutate the request with proper URL and Authent
        HttpRequest<?> mutatedRequest = mutateKafkaConnectRequest(request, connectConfig);
        // call it
        return client.proxy(mutatedRequest);
        // If required to modify the response, use this
        /* return Publishers.map(client.proxy(mutatedRequest),
                response -> response.header("X-My-Response-Header", "YYY"));*/
    }

    public MutableHttpRequest<?> mutateKafkaConnectRequest(HttpRequest<?> request, KafkaAsyncExecutorConfig.ConnectConfig connectConfig) {

        URI newURI = URI.create(connectConfig.getUrl());
        MutableHttpRequest<?> mutableHttpRequest = request.mutate()
                .uri(b -> b
                        .scheme(newURI.getScheme())
                        .host(newURI.getHost())
                        .port(newURI.getPort())
                        .replacePath(StringUtils.prependUri(
                                newURI.getPath(),
                                request.getPath().substring(KafkaConnectClientProxy.PROXY_PREFIX.length())
                        ))
                )
                .basicAuth(connectConfig.getBasicAuthUsername(), connectConfig.getBasicAuthPassword());

        // Micronaut resets Host later on with proper value.
        mutableHttpRequest.getHeaders().remove(HttpHeaders.HOST);
        return mutableHttpRequest;
    }
}
