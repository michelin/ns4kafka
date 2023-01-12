package com.michelin.ns4kafka.services.schema.client;

import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaRequest;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

import java.util.List;

@Client(value = KafkaSchemaRegistryClientProxy.SCHEMA_REGISTRY_PREFIX)
public interface KafkaSchemaRegistryClient {
    @Get("/subjects")
    Single<List<String>> getSubjects(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                     @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster);

    @Get("/subjects/{subject}/versions/latest")
    Maybe<SchemaResponse> getLatestSubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                           @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                           @PathVariable String subject);

    @Post("/subjects/{subject}/versions")
    Single<SchemaResponse> register(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                    @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                    @PathVariable String subject,
                                    @Body SchemaRequest request);

    @Delete("/subjects/{subject}")
    Single<Integer[]> deleteSubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                             @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                             @PathVariable String subject,
                                             @QueryValue("permanent") boolean hardDelete);

    @Post("/compatibility/subjects/{subject}/versions?verbose=true")
    Maybe<SchemaCompatibilityCheckResponse> validateSchemaCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                        @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                        @PathVariable String subject,
                                                                        @Body SchemaRequest request);

    @Put("/config/{subject}")
    Single<SchemaCompatibilityResponse> updateSubjectCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                    @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                    @PathVariable String subject,
                                    String compatibility);

    @Get("/config/{subject}")
    Maybe<SchemaCompatibilityResponse> getCurrentCompatibilityBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                           @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                           @PathVariable String subject);

    @Delete("/config/{subject}")
    Single<SchemaCompatibilityResponse> deleteCurrentCompatibilityBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                             @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                             @PathVariable String subject);
}
