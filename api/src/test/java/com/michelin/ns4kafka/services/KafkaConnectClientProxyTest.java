package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig.ConnectConfig;
import com.michelin.ns4kafka.services.connect.KafkaConnectClientProxy;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.http.*;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.simple.SimpleHttpRequest;
import io.reactivex.subscribers.TestSubscriber;
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
import java.util.stream.Stream;

@ExtendWith(MockitoExtension.class)
public class KafkaConnectClientProxyTest {
    @Mock
    ProxyHttpClient client;
    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @InjectMocks
    KafkaConnectClientProxy proxy;

    @Test
    void doFilterMissingHeader_Secret() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Unused", "123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Missing required Header X-Proxy-Secret")
        );
    }
    @Test
    void doFilterWrongSecret() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", "123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Invalid value 123 for Header X-Proxy-Secret")
        );
    }
    @Test
    void doFilterMissingHeader_KafkaCluster() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", KafkaConnectClientProxy.PROXY_SECRET)
                .header("X-Unused", "123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Missing required Header X-Kafka-Cluster")
        );
    }
    @Test
    void doFilterMissingHeader_ConnectCluster() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", KafkaConnectClientProxy.PROXY_SECRET)
                .header(KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable ->
                ((ResourceValidationException)throwable)
                        .getValidationErrors()
                        .contains("Missing required Header X-Connect-Cluster")
        );
    }

    @Test
    void doFilterWrongKafkaCluster() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", KafkaConnectClientProxy.PROXY_SECRET)
                .header(KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local")
                .header(KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER, "local-name");
        Mockito.when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.empty());

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

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
                .header("X-Proxy-Secret", KafkaConnectClientProxy.PROXY_SECRET)
                .header(KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local")
                .header(KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER, "local-name");
        KafkaAsyncExecutorConfig config = new KafkaAsyncExecutorConfig("local");
        ConnectConfig connectConfig = new KafkaAsyncExecutorConfig.ConnectConfig();
        config.setConnects(Map.of("invalid-name",connectConfig));

        Mockito.when(kafkaAsyncExecutorConfigs.stream())
                .thenReturn(Stream.of(config));

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(throwable ->
             ((ResourceValidationException)throwable)
                     .getValidationErrors()
                     .contains("Connect Cluster [local-name] not found")
        );

    }

    @Test
    void doFilterSuccess() {

        MutableHttpRequest<?> request = new MutableSimpleHttpRequest("http://localhost/connect-proxy/connectors")
                .header("X-Proxy-Secret", KafkaConnectClientProxy.PROXY_SECRET)
                .header(KafkaConnectClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local")
                .header(KafkaConnectClientProxy.PROXY_HEADER_CONNECT_CLUSTER, "local-name");
        KafkaAsyncExecutorConfig config1 = new KafkaAsyncExecutorConfig("local");
        ConnectConfig connectConfig = new KafkaAsyncExecutorConfig.ConnectConfig();
        connectConfig.setUrl("http://target/");
        config1.setConnects(Map.of("local-name",connectConfig));
        // Should not interfere
        KafkaAsyncExecutorConfig config2 = new KafkaAsyncExecutorConfig("not-match");

        Mockito.when(kafkaAsyncExecutorConfigs.stream())
                .thenReturn(Stream.of(config1, config2));
        Mockito.when(client.proxy(ArgumentMatchers.any(MutableHttpRequest.class)))
                .thenReturn(Publishers.just(HttpResponse.ok()));

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertValueCount(1);
        subscriber.assertValue(mutableHttpResponse -> mutableHttpResponse.status() == HttpStatus.OK);
    }

    @Test
    void testMutateKafkaConnectRequest() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest("http://localhost/connect-proxy/connectors");
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig();
        config.setUrl("http://target/");

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, config);

        Assertions.assertEquals("http://target/connectors", actual.getUri().toString());
    }

    @Test
    void testMutateKafkaConnectRequestRewrite() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest("http://localhost/connect-proxy/connectors");
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig();
        config.setUrl("http://target/rewrite");

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, config);

        Assertions.assertEquals("http://target/rewrite/connectors", actual.getUri().toString());
    }

    @Test
    void testMutateKafkaConnectRequestAuthent() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest("http://localhost/connect-proxy/connectors");
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig();
        config.setUrl("http://target/");
        config.setBasicAuthUsername("toto");
        config.setBasicAuthPassword("titi");

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, config);

        Assertions.assertEquals("http://target/connectors", actual.getUri().toString());
        Assertions.assertEquals("Basic dG90bzp0aXRp", actual.getHeaders().get(HttpHeaders.AUTHORIZATION));
    }

    public class MutableSimpleHttpRequest<B> extends SimpleHttpRequest<B>{

        @Override
        public MutableHttpRequest<B> mutate() {
            return this;
        }

        public MutableSimpleHttpRequest(String uri){
            super(HttpMethod.GET,uri,null);
        }
    }
}
