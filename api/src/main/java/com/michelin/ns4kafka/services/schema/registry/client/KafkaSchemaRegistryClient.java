package com.michelin.ns4kafka.services.schema.registry.client;

import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibilityConfig;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibilityCheck;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

@Client(KafkaSchemaRegistryClientProxy.SCHEMA_REGISTRY_PREFIX)
public interface KafkaSchemaRegistryClient {
    @Post("/subjects/{subject}/versions")
    void publish(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                 @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                 @PathVariable String subject,
                 @Body Schema.SchemaSpec.Content schema);

    @Post("/compatibility/subjects/{subject}/versions/latest?verbose=true")
    HttpResponse<SchemaCompatibilityCheck> validateSchemaCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                       @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                       @PathVariable String subject,
                                                                       @Body Schema.SchemaSpec.Content schema);

    @Put("/config/{subject}")
    void updateSchemaCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                     @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                     @PathVariable String subject,
                                                                     @Body SchemaCompatibilityConfig schemaCompatibilityConfig);

    @Delete("/subjects/{subject}")
    void deleteBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                   @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                   @PathVariable String subject,
                                   @QueryValue("permanent") boolean hardDelete);
}
