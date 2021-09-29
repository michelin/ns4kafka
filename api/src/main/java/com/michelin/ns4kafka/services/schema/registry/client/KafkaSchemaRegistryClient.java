package com.michelin.ns4kafka.services.schema.registry.client;

import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibility;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;

@Client(KafkaSchemaRegistryClientProxy.SCHEMA_REGISTRY_PREFIX)
public interface KafkaSchemaRegistryClient {
    @Post("/subjects/{subject}/versions")
    void publish(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                 @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                 @PathVariable String subject,
                 @Body Schema.SchemaSpec schema);

    @Post("/compatibility/subjects/{subject}/versions/latest?verbose=true")
    HttpResponse<SchemaCompatibility> compatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                               @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                               @PathVariable String subject,
                               @Body Schema.SchemaSpec schema);
}
