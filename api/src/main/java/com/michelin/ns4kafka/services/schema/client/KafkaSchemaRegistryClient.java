package com.michelin.ns4kafka.services.schema.client;

import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityCheckResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

import java.util.List;
import java.util.Map;

@Client(value = KafkaSchemaRegistryClientProxy.SCHEMA_REGISTRY_PREFIX)
public interface KafkaSchemaRegistryClient {
    @Get("/subjects")
    HttpResponse<List<String>> getSubjects(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                           @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster);

    @Get("/subjects/{subject}/versions/{version}")
    HttpResponse<SchemaResponse> getSchemaBySubjectAndVersion(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                              @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                              @PathVariable String subject,
                                                              @PathVariable String version);

    @Post("/subjects/{subject}/versions")
    HttpResponse<SchemaResponse> register(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                          @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                          @PathVariable String subject,
                                          @Body Map<String, String> schema);

    @Delete("/subjects/{subject}")
    void deleteSubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                         @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                         @PathVariable String subject,
                         @QueryValue("permanent") boolean hardDelete);

    @Post("/compatibility/subjects/{subject}/versions/latest?verbose=true")
    HttpResponse<SchemaCompatibilityCheckResponse> validateSchemaCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                               @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                               @PathVariable String subject,
                                                                               @Body Map<String, String> schema);

    @Put("/config/{subject}")
    void updateSubjectCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                    @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                    @PathVariable String subject,
                                    @Body Map<String, String> compatibility);

    @Get("/config/{subject}")
    HttpResponse<SchemaCompatibilityResponse> getCurrentCompatibilityBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                               @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                               @PathVariable String subject);

    @Delete("/config/{subject}")
    void deleteCurrentCompatibilityBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                             @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                             @PathVariable String subject);
}
