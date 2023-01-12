package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig.ConnectConfig;
import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.models.ConnectCluster;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.connect.ConnectorClientProxy;
import com.michelin.ns4kafka.utils.EncryptionUtils;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.http.*;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.simple.SimpleHttpRequest;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@ExtendWith(MockitoExtension.class)
class ConnectorClientProxyTest {
    @Mock
    ProxyHttpClient client;

    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @Mock
    ConnectClusterService connectClusterService;

    @Mock
    SecurityConfig securityConfig;

    @InjectMocks
    ConnectorClientProxy proxy;

    @Test
    void doFilterMissingHeaderSecret() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Unused", "123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Missing required header X-Proxy-Secret")
        );
    }

    @Test
    void doFilterWrongSecret() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", "123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Invalid value 123 for header X-Proxy-Secret")
        );
    }
    @Test
    void doFilterMissingHeaderKafkaCluster() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", ConnectorClientProxy.PROXY_SECRET)
                .header("X-Unused", "123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Missing required header X-Kafka-Cluster")
        );
    }
    @Test
    void doFilterMissingHeaderConnectCluster() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", ConnectorClientProxy.PROXY_SECRET)
                .header(ConnectorClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Missing required header X-Connect-Cluster")
        );
    }

    @Test
    void doFilterWrongKafkaCluster() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", ConnectorClientProxy.PROXY_SECRET)
                .header(ConnectorClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local")
                .header(ConnectorClientProxy.PROXY_HEADER_CONNECT_CLUSTER, "local-name");
        Mockito.when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.empty());

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Kafka Cluster [local] not found")
        );
    }
    @Test
    void doFilterWrongConnectCluster() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", ConnectorClientProxy.PROXY_SECRET)
                .header(ConnectorClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local")
                .header(ConnectorClientProxy.PROXY_HEADER_CONNECT_CLUSTER, "local-name");
        KafkaAsyncExecutorConfig config = new KafkaAsyncExecutorConfig("local");
        ConnectConfig connectConfig = new KafkaAsyncExecutorConfig.ConnectConfig();
        config.setConnects(Map.of("invalid-name",connectConfig));

        Mockito.when(kafkaAsyncExecutorConfigs.stream())
                .thenReturn(Stream.of(config));

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(throwable ->
             ((ResourceValidationException)throwable)
                     .getValidationErrors()
                     .contains("Connect cluster [local-name] not found")
        );

    }

    @Test
    void doFilterSuccess() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", ConnectorClientProxy.PROXY_SECRET)
                .header(ConnectorClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local")
                .header(ConnectorClientProxy.PROXY_HEADER_CONNECT_CLUSTER, "local-name");
        KafkaAsyncExecutorConfig config1 = new KafkaAsyncExecutorConfig("local");
        ConnectConfig connectConfig = new KafkaAsyncExecutorConfig.ConnectConfig();
        connectConfig.setUrl("https://target/");
        config1.setConnects(Map.of("local-name",connectConfig));
        // Should not interfere
        KafkaAsyncExecutorConfig config2 = new KafkaAsyncExecutorConfig("not-match");

        Mockito.when(kafkaAsyncExecutorConfigs.stream())
                .thenReturn(Stream.of(config1, config2));
        Mockito.when(client.proxy(ArgumentMatchers.any(MutableHttpRequest.class)))
                .thenReturn(Publishers.just(HttpResponse.ok()));

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertValueCount(1);
        subscriber.assertValue(mutableHttpResponse -> mutableHttpResponse.status() == HttpStatus.OK);
    }

    @Test
    void doFilterSuccessSelfDeployedConnectCluster() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", ConnectorClientProxy.PROXY_SECRET)
                .header(ConnectorClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local")
                .header(ConnectorClientProxy.PROXY_HEADER_CONNECT_CLUSTER, "connect-cluster");

        KafkaAsyncExecutorConfig config1 = new KafkaAsyncExecutorConfig("local");
        ConnectConfig connectConfig = new KafkaAsyncExecutorConfig.ConnectConfig();
        connectConfig.setUrl("https://target/");
        config1.setConnects(Map.of("local-name",connectConfig));
        KafkaAsyncExecutorConfig config2 = new KafkaAsyncExecutorConfig("not-match");

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://my-custom-connect-cluster")
                        .username("myUsername")
                        .password(EncryptionUtils.encryptAES256GCM("myPassword", "changeitchangeitchangeitchangeit"))
                        .build())
                .build();

        Mockito.when(kafkaAsyncExecutorConfigs.stream())
                .thenReturn(Stream.of(config1, config2));
        Mockito.when(connectClusterService.findAll())
                .thenReturn(List.of(connectCluster));
        Mockito.when(client.proxy(ArgumentMatchers.any(MutableHttpRequest.class)))
                .thenReturn(Publishers.just(HttpResponse.ok()));
        Mockito.when(securityConfig.getAes256EncryptionKey())
                .thenReturn("changeitchangeitchangeitchangeit");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertValueCount(1);
        subscriber.assertValue(mutableHttpResponse -> mutableHttpResponse.status() == HttpStatus.OK);
    }

    @Test
    void testMutateKafkaConnectRequest() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/connect-proxy/connectors");

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, "https://target/", null, null);

        Assertions.assertEquals("https://target/connectors", actual.getUri().toString());
    }

    @Test
    void testMutateKafkaConnectRequestEmptyHostHeader() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/connect-proxy/connectors");
        request.header("Host","value");

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, "https://target/", null, null);

        Assertions.assertEquals("https://target/connectors", actual.getUri().toString());
        Assertions.assertTrue(actual.getHeaders().getAll("Host").isEmpty(), "Host header should be unset");
    }

    @Test
    void testMutateKafkaConnectRequestRewrite() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/connect-proxy/connectors");

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, "https://target/rewrite", null, null);

        Assertions.assertEquals("https://target/rewrite/connectors", actual.getUri().toString());
    }

    @Test
    void testMutateKafkaConnectRequestAuthentication() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/connect-proxy/connectors");

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, "https://target/", "toto", "titi");

        Assertions.assertEquals("https://target/connectors", actual.getUri().toString());
        Assertions.assertEquals("Basic dG90bzp0aXRp", actual.getHeaders().get(HttpHeaders.AUTHORIZATION));
    }

    public static class MutableSimpleHttpRequest<B> extends SimpleHttpRequest<B>{

        @Override
        public MutableHttpRequest<B> mutate() {
            return this;
        }

        public MutableSimpleHttpRequest(String uri){
            super(HttpMethod.GET,uri,null);
        }
    }
}
