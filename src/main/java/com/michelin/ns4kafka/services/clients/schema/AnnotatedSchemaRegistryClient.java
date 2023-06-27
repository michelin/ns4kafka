package com.michelin.ns4kafka.services.clients.schema;

import com.michelin.ns4kafka.services.clients.schema.entities.SchemaResponse;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.client.annotation.Client;
import reactor.core.publisher.Mono;

@Client
public interface AnnotatedSchemaRegistryClient {
    @Get("/subjects/{subject}/versions/latest")
    Mono<SchemaResponse> getLatestSubject(@PathVariable String subject);
}
