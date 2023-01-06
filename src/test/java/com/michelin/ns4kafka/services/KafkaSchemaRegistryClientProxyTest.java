package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import io.micronaut.http.*;
import io.micronaut.http.client.ProxyHttpClient;
import io.micronaut.http.simple.SimpleHttpRequest;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.micronaut.core.async.publisher.Publishers.just;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaSchemaRegistryClientProxyTest {
    @Mock
    ProxyHttpClient client;

    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @InjectMocks
    KafkaSchemaRegistryClientProxy proxy;

    /**
     * Assert an exception is thrown when the secret header is missing
     */
    @Test
    void doFilterMissingSecretHeader() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/schema-registry-proxy/noHeader");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable -> ((ResourceValidationException) throwable).getValidationErrors().contains("Missing required header X-Proxy-Secret"));
    }

    /**
     * Assert an exception is thrown when the secret header is wrong
     */
    @Test
    void doFilterWrongSecret() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/schema-registry-proxy/wrongSecret")
                .header(KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET, "123");

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable -> ((ResourceValidationException) throwable).getValidationErrors().contains("Invalid value 123 for header X-Proxy-Secret"));
    }

    /**
     * Assert an exception is thrown when the Kafka cluster header is missing
     */
    @Test
    void doFilterMissingKafkaClusterHeader() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/schema-registry-proxy/noKafkaCluster")
                .header(KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET, KafkaSchemaRegistryClientProxy.PROXY_SECRET);

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(ResourceValidationException.class);
        subscriber.assertError(throwable -> ((ResourceValidationException) throwable).getValidationErrors().contains("Missing required header X-Kafka-Cluster"));
    }

    /**
     * Assert an exception is thrown when the Kafka cluster given in header is not found in the configuration
     */
    @Test
    void doFilterNoKafkaClusterFoundInConfig() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/schema-registry-proxy/noKafkaClusterInConfig")
                .header(KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET, KafkaSchemaRegistryClientProxy.PROXY_SECRET)
                .header(KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local");

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.empty());

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(throwable -> ((ResourceValidationException) throwable).getValidationErrors().contains("Kafka Cluster [local] not found"));
    }

    /**
     * Assert an exception is thrown when the Kafka cluster given in header is not found in the configuration
     */
    @Test
    void doFilterNoSchemaRegistryFoundInConfig() {
        MutableHttpRequest<?> request = HttpRequest
                .GET("http://localhost/schema-registry-proxy/noSchemaRegistryInConfig")
                .header(KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET, KafkaSchemaRegistryClientProxy.PROXY_SECRET)
                .header(KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local");

        when(kafkaAsyncExecutorConfigs.stream()).thenReturn(Stream.of(new KafkaAsyncExecutorConfig("local")));

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertError(throwable -> ((ResourceValidationException) throwable).getValidationErrors().contains("Kafka Cluster [local] has no schema registry"));
    }

    /**
     * Test the filter is working
     */
    @Test
    void doFilterSuccess() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/schema-registry-proxy/success")
                .header(KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET, KafkaSchemaRegistryClientProxy.PROXY_SECRET)
                .header(KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER, "local");

        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig("local");
        KafkaAsyncExecutorConfig.RegistryConfig registryConfig = new KafkaAsyncExecutorConfig.RegistryConfig();
        registryConfig.setUrl("http://schema-registry");
        kafkaAsyncExecutorConfig.setSchemaRegistry(registryConfig);

        when(kafkaAsyncExecutorConfigs.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig));

        when(client.proxy(any(MutableHttpRequest.class)))
                .thenReturn(just(HttpResponse.ok()));

        TestSubscriber<MutableHttpResponse<?>> subscriber = new TestSubscriber<>();
        Publisher<MutableHttpResponse<?>> mutableHttpResponsePublisher = proxy.doFilter(request, null);

        mutableHttpResponsePublisher.subscribe(subscriber);
        subscriber.awaitDone(1L, TimeUnit.SECONDS);

        subscriber.assertValueCount(1);
        subscriber.assertValue(mutableHttpResponse -> mutableHttpResponse.status() == HttpStatus.OK);
    }

    /**
     * Test the mutation of Schema Registry requests
     *
     * The mutation should modify the URI of the original request
     */
    @Test
    void mutateSchemaRegistryRequest() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/schema-registry-proxy/success");

        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig("local");
        KafkaAsyncExecutorConfig.RegistryConfig registryConfig = new KafkaAsyncExecutorConfig.RegistryConfig();
        registryConfig.setUrl("http://schema-registry");
        kafkaAsyncExecutorConfig.setSchemaRegistry(registryConfig);

        MutableHttpRequest<?> actual = proxy.mutateSchemaRegistryRequest(request, kafkaAsyncExecutorConfig);

        Assertions.assertEquals("http", actual.getUri().getScheme());
        Assertions.assertEquals("schema-registry", actual.getUri().getHost());
        Assertions.assertEquals("http://schema-registry/success", actual.getUri().toString());
        Assertions.assertNull(actual.getHeaders().get(HttpHeaders.AUTHORIZATION));
    }

    /**
     * Test the mutation of Schema Registry requests with Basic Auth
     */
    @Test
    void mutateSchemaRegistryRequestWithBasicAuth() {
        MutableHttpRequest<?> request = new MutableSimpleHttpRequest<>("http://localhost/schema-registry-proxy/success");

        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig("local");
        KafkaAsyncExecutorConfig.RegistryConfig registryConfig = new KafkaAsyncExecutorConfig.RegistryConfig();
        registryConfig.setUrl("http://schema-registry");
        registryConfig.setBasicAuthUsername("username");
        registryConfig.setBasicAuthPassword("password");
        kafkaAsyncExecutorConfig.setSchemaRegistry(registryConfig);

        MutableHttpRequest<?> actual = proxy.mutateSchemaRegistryRequest(request, kafkaAsyncExecutorConfig);

        Assertions.assertEquals("http", actual.getUri().getScheme());
        Assertions.assertEquals("schema-registry", actual.getUri().getHost());
        Assertions.assertEquals("http://schema-registry/success", actual.getUri().toString());
        Assertions.assertEquals("Basic dXNlcm5hbWU6cGFzc3dvcmQ=", actual.getHeaders().get(HttpHeaders.AUTHORIZATION));
    }

    /**
     * Implementation of a mutable HTTP request for tests
     * @param <B>
     */
    public static class MutableSimpleHttpRequest<B> extends SimpleHttpRequest<B> {

        @Override
        public MutableHttpRequest<B> mutate() {
            return this;
        }

        public MutableSimpleHttpRequest(String uri){
            super(HttpMethod.GET,uri,null);
        }
    }
}
