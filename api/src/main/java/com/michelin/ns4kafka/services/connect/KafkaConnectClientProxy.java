package com.michelin.ns4kafka.services.connect;

import com.michelin.ns4kafka.services.KafkaAsyncExecutorConfig;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.filter.OncePerRequestHttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import java.net.URI;
import java.util.List;

@Filter(KafkaConnectClientProxy.CONNECT_PROXY_PREFIX + "/**")
public class KafkaConnectClientProxy extends OncePerRequestHttpServerFilter {
    public static final String CONNECT_PROXY_PREFIX = "/connect-proxy";

    @Inject
    ProxyHttpClient client;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Override
    protected Publisher<MutableHttpResponse<?>> doFilterOnce(HttpRequest<?> request, ServerFilterChain chain) {
        // retrieve the kafka connect url for this cluster
        String cluster = request.getHeaders().get("X-Connect-Cluster");
        KafkaAsyncExecutorConfig config = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(cluster))
                .findFirst().get();
        URI newURI = URI.create(config.getConnect().getUrl());

        // call kafka connect with proper URL and Auth
        return Publishers.map(client.proxy(
                request.mutate()
                        .uri(b -> b
                                .scheme(newURI.getScheme())
                                .host(newURI.getHost())
                                .port(newURI.getPort())
                                .replacePath(StringUtils.prependUri(
                                        newURI.getPath(),
                                        request.getPath().substring(KafkaConnectClientProxy.CONNECT_PROXY_PREFIX.length())
                                ))
                        )
                        .basicAuth(config.getConnect().getBasicAuthUsername(), config.getConnect().getBasicAuthPassword())
        ), response -> response.header("X-My-Response-Header", "YYY"));
    }
}
