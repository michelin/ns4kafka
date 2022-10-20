package com.michelin.ns4kafka.services.connect;

import com.michelin.ns4kafka.models.ConnectCluster;
import com.michelin.ns4kafka.services.ConnectClusterService;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig.ConnectConfig;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.filter.OncePerRequestHttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Filter(ConnectorClientProxy.PROXY_PREFIX + "/**")
public class ConnectorClientProxy extends OncePerRequestHttpServerFilter {
    /**
     * Prefix used to filter request to Connect clusters. It'll be replaced by
     * the Connect cluster URL of the given cluster
     */
    public static final String PROXY_PREFIX = "/connect-proxy";

    /**
     * A header that contains the Kafka cluster
     */
    public static final String PROXY_HEADER_KAFKA_CLUSTER = "X-Kafka-Cluster";

    /**
     * A header that contains the Connect cluster name
     */
    public static final String PROXY_HEADER_CONNECT_CLUSTER = "X-Connect-Cluster";

    /**
     * A header that contains a secret for the request
     */
    public static final String PROXY_HEADER_SECRET = "X-Proxy-Secret";

    /**
     * Generate a secret
     */
    public static final String PROXY_SECRET = UUID.randomUUID().toString();

    @Inject
    ProxyHttpClient client;

    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Inject
    ConnectClusterService connectClusterService;

    /**
     * Filter requests
     * @param request The request to filter
     * @param chain The servlet chain
     * @return A modified request
     */
    @Override
    public Publisher<MutableHttpResponse<?>> doFilterOnce(HttpRequest<?> request, ServerFilterChain chain) {
        // Check call is initiated from Micronaut and not from outside
        if (!request.getHeaders().contains(ConnectorClientProxy.PROXY_HEADER_SECRET)) {
            return Publishers.just(new ResourceValidationException(List.of("Missing required header " + ConnectorClientProxy.PROXY_HEADER_SECRET), null, null));
        }

        String secret = request.getHeaders().get(ConnectorClientProxy.PROXY_HEADER_SECRET);
        if (!PROXY_SECRET.equals(secret)) {
            return Publishers.just(new ResourceValidationException(List.of("Invalid value " + secret + " for header " + ConnectorClientProxy.PROXY_HEADER_SECRET), null, null));
        }

        if (!request.getHeaders().contains(ConnectorClientProxy.PROXY_HEADER_KAFKA_CLUSTER)) {
            return Publishers.just(new ResourceValidationException(List.of("Missing required header " + ConnectorClientProxy.PROXY_HEADER_KAFKA_CLUSTER), null, null));
        }

        if (!request.getHeaders().contains(ConnectorClientProxy.PROXY_HEADER_CONNECT_CLUSTER)) {
            return Publishers.just(new ResourceValidationException(List.of("Missing required header " + ConnectorClientProxy.PROXY_HEADER_CONNECT_CLUSTER), null, null));
        }

        String kafkaCluster = request.getHeaders().get(ConnectorClientProxy.PROXY_HEADER_KAFKA_CLUSTER);
        String connectCluster = request.getHeaders().get(ConnectorClientProxy.PROXY_HEADER_CONNECT_CLUSTER);

        // Get config of the kafkaCluster
        Optional<KafkaAsyncExecutorConfig> config = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster))
                .findFirst();

        if (config.isEmpty()) {
            return Publishers.just(new ResourceValidationException(List.of("Kafka Cluster [" + kafkaCluster + "] not found"),null,null));
        }

        Optional<ConnectCluster> connectClusterOptional = connectClusterService.findByName(connectCluster);
        if (connectClusterOptional.isPresent()) {
            log.debug("Self deployed Connect cluster {} found in namespace {}", connectCluster, connectClusterOptional.get().getMetadata().getNamespace());
            return client.proxy(mutateKafkaConnectRequest(request, connectClusterOptional.get().getSpec().getUrl(),
                    connectClusterOptional.get().getSpec().getUsername(),
                    connectClusterOptional.get().getSpec().getPassword()));
        }

        ConnectConfig connectConfig = config.get().getConnects().get(connectCluster);
        if (connectConfig == null) {
            return Publishers.just(new ResourceValidationException(List.of("Connect cluster [" + connectCluster + "] not found"), null, null));
        }

        log.debug("Connect cluster {} found in Ns4Kafka configuration", connectCluster);

        return client.proxy(mutateKafkaConnectRequest(request, connectConfig.getUrl(),
                connectConfig.getBasicAuthUsername(),
                connectConfig.getBasicAuthPassword()));
    }

    /**
     * Mutate the prefixed request to the required Connect cluster, either from the Ns4Kafka configuration or from a self-deployed
     * Connect cluster configuration
     * @param request The request to modify
     * @param url The Connect cluster URL
     * @param username The Connect cluster username
     * @param password The Connect cluster password
     * @return The modified request
     */
    public MutableHttpRequest<?> mutateKafkaConnectRequest(HttpRequest<?> request, String url, String username, String password) {
        URI newURI = URI.create(url);

        MutableHttpRequest<?> mutableHttpRequest = request.mutate()
                .uri(b -> b
                        .scheme(newURI.getScheme())
                        .host(newURI.getHost())
                        .port(newURI.getPort())
                        .replacePath(StringUtils.prependUri(
                                newURI.getPath(),
                                request.getPath().substring(ConnectorClientProxy.PROXY_PREFIX.length())
                        ))
                )
                .basicAuth(username, password);

        // Micronaut resets Host later on with proper value.
        mutableHttpRequest.getHeaders().remove(HttpHeaders.HOST);
        return mutableHttpRequest;
    }
}
