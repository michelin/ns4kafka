package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.SchemaCompatibility;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

import java.util.List;
import java.util.Map;

@Client("${kafkactl.api}/api/namespaces/")
public interface NamespacedResourceClient {
    @Delete("{namespace}/{kind}/{resourceName}{?dryrun}")
    HttpResponse<Void> delete(
            String namespace,
            String kind,
            String resourceName,
            @Header("Authorization") String token,
            @QueryValue boolean dryrun);

    @Post("{namespace}/{kind}{?dryrun}")
    HttpResponse<Resource> apply(
            String namespace,
            String kind,
            @Header("Authorization") String token,
            @Body Resource json,
            @QueryValue boolean dryrun);

    @Get("{namespace}/{kind}")
    List<Resource> list(
            String namespace,
            String kind,
            @Header("Authorization") String token);

    @Get("{namespace}/{kind}/{resourceName}")
    Resource get(
            String namespace,
            String kind,
            String resourceName,
            @Header("Authorization") String token);

    @Post("{namespace}/{kind}/_/import{?dryrun}")
    List<Resource> importResources(
            String namespace,
            String kind,
            @Header("Authorization") String token,
            @QueryValue boolean dryrun);

    /**
     * Delete records for a given topic
     * @param token The authentication token
     * @param namespace The namespace
     * @param topic The topic to delete records
     * @param dryrun Is dry run mode or not ?
     * @return The deleted records response
     */
    @Post("{namespace}/topics/{topic}/delete-records{?dryrun}")
    List<Resource> deleteRecords(
            @Header("Authorization") String token,
            String namespace,
            String topic,
            @QueryValue boolean dryrun);

    /**
     * Reset offsets for a given topic and consumer group
     * @param token The authentication token
     * @param namespace The namespace
     * @param consumerGroupName The consumer group
     * @param json The information about how to reset
     * @param dryrun Is dry run mode or not ?
     * @return The reset offsets response
     */
    @Post("{namespace}/consumer-groups/{consumerGroupName}/reset{?dryrun}")
    List<Resource> resetOffsets(
            @Header("Authorization") String token,
            String namespace,
            String consumerGroupName,
            @Body Resource json,
            @QueryValue boolean dryrun);

    @Post("{namespace}/connects/{connector}/change-state")
    Resource changeConnectorState(
            String namespace,
            String connector,
            @Body Resource changeConnectorState,
            @Header("Authorization") String token);

    @Post("{namespace}/schemas/{subject}/config")
    Resource changeSchemaCompatibility(
            String namespace,
            String subject,
            @Body Map<String, SchemaCompatibility> compatibility,
            @Header("Authorization") String token);

    @Post("{namespace}/users/{user}/reset-password")
    Resource resetPassword(String namespace, String user, @Header("Authorization") String token);
}
