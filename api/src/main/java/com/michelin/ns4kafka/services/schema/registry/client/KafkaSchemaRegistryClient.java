package com.michelin.ns4kafka.services.schema.registry.client;

import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaResponse;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityRequest;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

import java.util.List;

@Client(KafkaSchemaRegistryClientProxy.SCHEMA_REGISTRY_PREFIX)
public interface KafkaSchemaRegistryClient {
    @Post("/subjects/{subject}/versions")
    void publish(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                 @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                 @PathVariable String subject,
                 @Body Subject.SubjectSpec.Content schema);

    @Get("/config/{subject}")
    HttpResponse<SubjectCompatibilityResponse> getCurrentCompatibilityBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                                @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                                @PathVariable String subject,
                                                                                @QueryValue("defaultToGlobal") boolean defaultToGlobal);

    @Post("/compatibility/subjects/{subject}/versions/latest?verbose=true")
    HttpResponse<SubjectCompatibilityCheckResponse> validateSubjectCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                                @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                                @PathVariable String subject,
                                                                                @Body Subject.SubjectSpec.Content schema);

    @Put("/config/{subject}")
    void updateSubjectCompatibility(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                     @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                     @PathVariable String subject,
                                                                     @Body SubjectCompatibilityRequest subjectCompatibilityRequest);

    @Delete("/subjects/{subject}")
    void deleteBySubject(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                         @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                         @PathVariable String subject,
                         @QueryValue("permanent") boolean hardDelete);

    @Get("/subjects")
    HttpResponse<List<String>> getAllSubjects(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                              @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster);

    @Get("/subjects/{subject}/versions/{version}")
    HttpResponse<SchemaResponse> getSchemaBySubjectAndVersion(@Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_SECRET) String secret,
                                                                    @Header(value = KafkaSchemaRegistryClientProxy.PROXY_HEADER_KAFKA_CLUSTER) String cluster,
                                                                    @PathVariable String subject,
                                                                    @PathVariable String version);
}
