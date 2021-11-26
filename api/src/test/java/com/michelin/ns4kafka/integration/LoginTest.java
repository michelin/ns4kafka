package com.michelin.ns4kafka.integration;

import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.rxjava3.http.client.Rx3HttpClient;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class LoginTest extends AbstractIntegrationTest {

    @Inject
    @Client("/")
    Rx3HttpClient client;
    @Test
    void login(){

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<String> response = client.exchange(HttpRequest.POST("/login", credentials), String.class).blockingFirst();
        Assertions.assertEquals(HttpStatus.OK, response.status());
    }
}
