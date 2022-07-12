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
    /**
     * Delete a given resource
     * @param namespace The namespace
     * @param kind The kind of resource
     * @param resourceName The name of the resource
     * @param token The auth token
     * @param dryrun is dry-run mode or not ?
     * @return The delete response
     */
    @Delete("{namespace}/{kind}/{resourceName}{?dryrun}")
    HttpResponse<Void> delete(
            String namespace,
            String kind,
            String resourceName,
            @Header("Authorization") String token,
            @QueryValue boolean dryrun);

    /**
     * Apply a given resource
     * @param namespace The namespace
     * @param kind The kind of resource
     * @param token The auth token
     * @param resource The resource to apply
     * @param dryrun is dry-run mode or not ?
     * @return The resource
     */
    @Post("{namespace}/{kind}{?dryrun}")
    HttpResponse<Resource> apply(
            String namespace,
            String kind,
            @Header("Authorization") String token,
            @Body Resource json,
            @QueryValue boolean dryrun);

    /**
     * List all resources
     * @param namespace The namespace
     * @param kind The kind of resource
     * @param token The auth token
     * @return The list of resources
     */
    @Get("{namespace}/{kind}")
    List<Resource> list(
            String namespace,
            String kind,
            @Header("Authorization") String token);

    /**
     * Get a resource
     * @param namespace The namespace
     * @param kind The kind of resource
     * @param resourceName The name of the resource
     * @param token The auth token
     * @return The resource
     */
    @Get("{namespace}/{kind}/{resourceName}")
    Resource get(
            String namespace,
            String kind,
            String resourceName,
            @Header("Authorization") String token);

    /**
     * Imports the unsynchronized given type of resource
     * @param namespace The namespace
     * @param kind The kind of resource
     * @param token The auth token
     * @param dryrun is dry-run mode or not ?
     * @return The list of imported resources
     */
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

    /**
     * Change the state of a given connector
     * @param namespace The namespace
     * @param connector The connector to change
     * @param changeConnectorState The state
     * @param token The auth token
     * @return The change state response
     */
    @Post("{namespace}/connects/{connector}/change-state")
    Resource changeConnectorState(
            String namespace,
            String connector,
            @Body Resource changeConnectorState,
            @Header("Authorization") String token);

    /**
     * Change the schema compatibility mode
     * @param namespace The namespace
     * @param subject The subject
     * @param compatibility The compatibility to apply
     * @param token The auth token
     * @return The change compatibility response
     */
    @Post("{namespace}/schemas/{subject}/config")
    Resource changeSchemaCompatibility(
            String namespace,
            String subject,
            @Body Map<String, SchemaCompatibility> compatibility,
            @Header("Authorization") String token);

    /**
     * Reset password of a given user
     * @param namespace The namespace
     * @param user The user
     * @param token The auth token
     * @return The reset password response
     */
    @Post("{namespace}/users/{user}/reset-password")
    Resource resetPassword(String namespace, String user, @Header("Authorization") String token);
}
