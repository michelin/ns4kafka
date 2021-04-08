package com.michelin.ns4kafka.cli.client;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;

@Client("http://localhost:8080/api/namespaces/")
public interface NonNamespacedResourceClient {

    @Delete("/")
    void delete(

            @Header(name = "Authorization", value = "Authorization") String token);

    @Post("/")
    String apply(
            @Header(name = "Authorization", value = "Authorization") String token,
            @Body String json);

    @Get("/")
    String list();
}
