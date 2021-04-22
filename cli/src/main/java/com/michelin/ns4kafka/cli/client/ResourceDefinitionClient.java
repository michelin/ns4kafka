package com.michelin.ns4kafka.cli.client;

import java.util.List;

import com.michelin.ns4kafka.cli.models.ResourceDefinition;

import io.micronaut.http.annotation.Get;
import io.micronaut.http.client.annotation.Client;

@Client("${api.server}")
public interface ResourceDefinitionClient {

    @Get("/api-resources")
    List<ResourceDefinition> getResource();
}
