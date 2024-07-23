package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class LoginIntegrationTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void shouldLoginWithJwt() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<String> response = client.toBlocking()
            .exchange(HttpRequest.POST("/login", credentials), String.class);
        assertEquals(HttpStatus.OK, response.status());
    }

    @Test
    void shouldLoginWithBasicAuth() {
        HttpResponse<String> response = client.toBlocking()
            .exchange(HttpRequest.GET("/api/namespaces")
                .basicAuth("admin", "admin"), String.class);
        assertEquals(HttpStatus.OK, response.status());
    }
}
