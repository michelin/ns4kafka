package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.services.KafkaAsyncExecutorConfig.ConnectConfig;
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
    void doFilterMissingHeader() {
        HttpRequest request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Unused", "123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(Exception.class);
        subscriber.assertError(throwable -> throwable.getClass().equals(Exception.class));
        subscriber.assertErrorMessage("Missing required Header X-Connect-Cluster");
    }

    @Test
    void doFilterMissingConnectConfig() {
        HttpRequest request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Connect-Cluster", "local")
                .header("X-Connect-Name", "local-name");
        Mockito.when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.empty());

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(Exception.class);
        subscriber.assertError(throwable -> throwable.getClass().equals(Exception.class));
        subscriber.assertErrorMessage("No ConnectConfig found for cluster [local]");
    }

    @Test
    void doFilterSuccess() {

        MutableHttpRequest<?> request = new MutableSimpleHttpRequest("http://localhost/connect-proxy/connectors")
                .header("X-Connect-Cluster", "local")
                .header("X-Connect-Name", "local-name");
        KafkaAsyncExecutorConfig config1 = new KafkaAsyncExecutorConfig("local");
        ConnectConfig connectConfig = new KafkaAsyncExecutorConfig.ConnectConfig("local-name");
        connectConfig.url = "http://target/";
        connectConfig.basicAuthUsername = "toto";
        connectConfig.basicAuthPassword = "titi";
        config1.connects = Map.of("local-name",connectConfig);
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
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig("local-name");
        config.url = "http://target/";

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, config);

        Assertions.assertEquals("http://target/connectors", actual.getUri().toString());
    }

    @Test
    void testMutateKafkaConnectRequestRewrite() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest("http://localhost/connect-proxy/connectors");
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig("local-name");
        config.url = "http://target/rewrite";

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, config);

        Assertions.assertEquals("http://target/rewrite/connectors", actual.getUri().toString());
    }

    @Test
    void testMutateKafkaConnectRequestAuthent() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest("http://localhost/connect-proxy/connectors");
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig("local-name");
        config.url = "http://target/";
        config.basicAuthUsername = "toto";
        config.basicAuthPassword = "titi";

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
