package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.models.Resource;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

import java.util.List;

@Client("${kafkactl.api}/api/namespaces/")
public interface NamespacedResourceClient {

    @Delete("{namespace}/{kind}/{resourcename}")
    void delete(
            String namespace,
            String kind,
            String resourcename,
            @Header(name = "Authorization", value = "Authorization") String token);

    @Post("{namespace}/{kind}")
    Resource apply(
            String namespace,
            String kind,
            @Header(name = "Authorization", value = "Authorization") String token,
            @Body Resource json);

    @Get("{namespace}/{kind}")
    List<Resource> list(
            String namespace,
            String kind,
            @Header(name = "Authorization", value = "Authorization") String token);

    @Get("{namespace}/{kind}/{resourcename}")
    Resource get(
            String namespace,
            String kind,
            String resourcename,
            @Header(name = "Authorization", value = "Authorization") String token);
}
