package com.michelin.ns4kafka.integration;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

@MicronautTest
public class LoginTest {

    @Inject
    @Client("/")
    RxHttpClient client;

    @Test
    void login(){
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<String> response = client.exchange(HttpRequest.POST("/login", credentials), String.class).blockingFirst();
        Assertions.assertEquals(HttpStatus.OK, response.status());
    }
}
