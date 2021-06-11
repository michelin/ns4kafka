package com.michelin.ns4kafka.integration;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Testcontainers
public class LoginTest extends AbstractIntegrationTest{

    @Test
    void login(){
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<String> response = client.exchange(HttpRequest.POST("/login", credentials), String.class).blockingFirst();
        Assertions.assertEquals(HttpStatus.OK, response.status());
    }
}
