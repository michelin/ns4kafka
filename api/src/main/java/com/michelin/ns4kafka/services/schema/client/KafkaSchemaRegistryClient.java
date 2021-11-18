package com.michelin.ns4kafka.services.schema.client;

import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.entities.*;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

import java.util.List;
import java.util.Optional;

@Client(value = KafkaSchemaRegistryClientProxy.SCHEMA_REGISTRY_PREFIX)
public interface KafkaSchemaRegistryClient {
    @Get("/subjects")
    List<String> getSubjects(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                             @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster);

    @Get("/subjects/{subject}/versions/latest")
    Optional<SchemaResponse> getLatestSubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                              @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                              @PathVariable String subject);

    @Post("/subjects/{subject}/versions")
    SchemaResponse register(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                            @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                            @PathVariable String subject,
                            @Body SchemaRequest request);

    @Delete("/subjects/{subject}")
    void deleteSubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                         @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                         @PathVariable String subject,
                         @QueryValue("permanent") boolean hardDelete);

    @Post("/compatibility/subjects/{subject}/versions?verbose=true")
    Optional<SchemaCompatibilityCheckResponse> validateSchemaCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                 @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                 @PathVariable String subject,
                                                                 @Body SchemaRequest request);

    @Put("/config/{subject}")
    SchemaCompatibilityResponse updateSubjectCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                    @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                    @PathVariable String subject,
                                    @Body SchemaCompatibilityRequest request);

    @Get("/config/{subject}")
    Optional<SchemaCompatibilityResponse> getCurrentCompatibilityBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                           @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                           @PathVariable String subject);

    @Delete("/config/{subject}")
    void deleteCurrentCompatibilityBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                             @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                             @PathVariable String subject);
}
