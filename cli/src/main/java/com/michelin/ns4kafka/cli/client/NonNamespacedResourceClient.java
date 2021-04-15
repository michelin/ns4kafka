package com.michelin.ns4kafka.cli.client;

import java.util.List;

import com.michelin.ns4kafka.cli.models.Resource;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;

public interface NonNamespacedResourceClient extends ResourceClient {

    @Delete("/")
    void delete(
            @Header(name = "Authorization", value = "Authorization") String token);

    @Post("/")
    Resource apply(
            @Header(name = "Authorization", value = "Authorization") String token,
            @Body String json);

    @Get("/")
    List<Resource> list(
            @Header(name = "Authorization", value = "Authorization") String token);
}
