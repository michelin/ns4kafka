package com.michelin.ns4kafka.services.connect;

import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig.ConnectConfig;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.filter.OncePerRequestHttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Optional;

@Filter(KafkaConnectClientProxy.PROXY_PREFIX + "/**")
public class KafkaConnectClientProxy extends OncePerRequestHttpServerFilter {
    public static final String PROXY_PREFIX = "/connect-proxy";
    public static final String PROXY_HEADER_KAFKA_CLUSTER = "X-Kafka-Cluster";
    public static final String PROXY_HEADER_CONNECT_CLUSTER = "X-Connect-Cluster";

    @Inject
    ProxyHttpClient client;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Override
    public Publisher<MutableHttpResponse<?>> doFilterOnce(HttpRequest<?> request, ServerFilterChain chain) {
        // retrieve the connectConfig based on Header
        if (!request.getHeaders().contains(KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER)) {
            return Publishers.just(new Exception("Missing required Header " + KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER));
        }
        if (!request.getHeaders().contains(KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER)) {
            return Publishers.just(new Exception("Missing required Header " + KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER));
        }

        String kafkaCluster = request.getHeaders().get(KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER);
        String connectCluster = request.getHeaders().get(KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER);

        // get config of the kafkaCluster
        Optional<KafkaAsyncExecutorConfig> config = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
                .findFirst();
        if (config.isEmpty()) {
            return Publishers.just(new Exception("Kafka Cluster [" + kafkaCluster + "] not found"));
        }

        // get the good connect config
        ConnectConfig connectConfig = config.get().getConnects().get(connectCluster);
        if (connectConfig == null) {
            return Publishers.just(new Exception("Connect Cluster [" + connectCluster + "] not found"));
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
        return request.mutate()
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
    }
}
