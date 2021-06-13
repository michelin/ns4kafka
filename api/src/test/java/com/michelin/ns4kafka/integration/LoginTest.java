package com.michelin.ns4kafka.integration;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.inject.Inject;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
public class LoginTest implements TestPropertyProvider {

    @Inject
    @Client("/")
    RxHttpClient client;
    @Test
    void login(){

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<String> response = client.exchange(HttpRequest.POST("/login", credentials), String.class).blockingFirst();
        Assertions.assertEquals(HttpStatus.OK, response.status());
    }

    @NonNull
    @Override
    public Map<String, String> getProperties() {
        KafkaContainer.init();
        return Map.of(
                "kafka.bootstrap.servers", KafkaContainer.kafka.getBootstrapServers(),
                "ns4kafka.managed-clusters.test-cluster.config.bootstrap.servers", KafkaContainer.kafka.getBootstrapServers()
        );
    }
}
