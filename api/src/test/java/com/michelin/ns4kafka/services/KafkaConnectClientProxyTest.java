package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.services.connect.KafkaConnectClientProxy;
import io.micronaut.http.*;
import io.micronaut.http.client.ProxyHttpClient;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import java.util.List;
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
    void missingHeader(){
        HttpRequest request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Unused","123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request,null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(Exception.class);
        subscriber.assertError(throwable -> throwable.getClass().equals(Exception.class));
        subscriber.assertErrorMessage("Missing required Header X-Connect-Cluster");
    }
    @Test
    void missingConnectConfig(){
        HttpRequest request = HttpRequest
                .GET("http://localhost/connect-proxy/connectors")
                .header("X-Connect-Cluster","local");
        Mockito.when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.empty());

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilterOnce(request,null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        subscriber.assertError(Exception.class);
        subscriber.assertError(throwable -> throwable.getClass().equals(Exception.class));
        subscriber.assertErrorMessage("No ConnectConfig found for cluster [local]");
    }

    @Test
    void testMutateKafkaConnectRequest(){
        MutableHttpRequest<?> request = HttpRequestFactory.INSTANCE.get("http://localhost/connect-proxy/connectors");
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig();
        config.url = "http://target/";

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, config);

        Assertions.assertEquals("http://target/connectors", actual.getUri().toString());
    }

    @Test
    void testMutateKafkaConnectRequestRewrite(){
        MutableHttpRequest<?> request = HttpRequestFactory.INSTANCE.get("http://localhost/connect-proxy/connectors");
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig();
        config.url = "http://target/rewrite";

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, config);

        Assertions.assertEquals("http://target/rewrite/connectors", actual.getUri().toString());
    }

    @Test
    void testMutateKafkaConnectRequestAuthent(){
        MutableHttpRequest<?> request = HttpRequestFactory.INSTANCE.get("http://localhost/connect-proxy/connectors");
        KafkaAsyncExecutorConfig.ConnectConfig config = new KafkaAsyncExecutorConfig.ConnectConfig();
        config.url = "http://target/";
        config.basicAuthUsername = "toto";
        config.basicAuthPassword = "titi";

        MutableHttpRequest<?> actual = proxy.mutateKafkaConnectRequest(request, config);

        Assertions.assertEquals("http://target/connectors", actual.getUri().toString());
        Assertions.assertEquals("Basic dG90bzp0aXRp", actual.getHeaders().get(HttpHeaders.AUTHORIZATION));
    }
}
