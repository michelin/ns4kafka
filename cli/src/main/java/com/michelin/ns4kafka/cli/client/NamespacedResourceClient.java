package com.michelin.ns4kafka.cli.client;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;

public interface NamespacedResourceClient extends ResourceClient {

    @Delete("{namespace}/{kind}/{resourcename}")
    void delete(
            String namespace,
            String kind,
            String resourcename,
            @Header(name = "Authorization", value = "Authorization") String token);

    @Post("{namespace}/{kind}")
    String apply(
            String namespace,
            String kind,
            @Header(name = "Authorization", value = "Authorization") String token,
            @Body String json);

    @Get("{namespace}/{kind}")
    String list(
            String namespace,
            String kind,
            @Header(name = "Authorization", value = "Authorization") String token);

    @Get("{namespace}/{kind}/{resourcename}")
    String get(
            String namespace,
            String kind,
            String resourcename,
            @Header(name = "Authorization", value = "Authorization") String token);
}
