package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.models.Resource;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

import java.util.List;

@Client("${kafkactl.api}/api/namespaces/")
public interface NamespacedResourceClient {

    @Delete("{namespace}/{kind}/{resourcename}{?dryrun}")
    void delete(
            String namespace,
            String kind,
            String resourcename,
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

    @Get("{namespace}/{kind}/{resourcename}")
    Resource get(
            String namespace,
            String kind,
            String resourcename,
            @Header("Authorization") String token);

    @Post("{namespace}/{kind}/_/import{?dryrun}")
    List<Resource> importResources(
            String namespace,
            String kind,
            @Header("Authorization") String token,
            @QueryValue boolean dryrun);

    @Post("{namespace}/topics/{topic}/delete-records{?dryrun}")
    Resource deleteRecords(
            @Header("Authorization") String token,
            String namespace,
            String topic,
            @QueryValue boolean dryrun);

    @Post("{namespace}/consumer-groups/{consumerGroupName}/reset{?dryrun}")
    Resource resetOffsets(
            @Header("Authorization") String token,
            String namespace,
            String consumerGroupName,
            @Body Resource json,
            @QueryValue boolean dryrun);

    @Post("{namespace}/connects/{resourcename}/restart{?dryrun}")
    HttpResponse<Void> restartConnect(
            String namespace,
            String resourcename,
            @Header("Authorization") String token,
            @QueryValue boolean dryrun);

    @Post("{namespace}/connects/{resourcename}/pause{?dryrun}")
    HttpResponse<Void> pauseConnect(
            String namespace,
            String resourcename,
            @Header("Authorization") String token,
            @QueryValue boolean dryrun);

    @Post("{namespace}/connects/{resourcename}/resume{?dryrun}")
    HttpResponse<Void> resumeConnect(
            String namespace,
            String resourcename,
            @Header("Authorization") String token,
            @QueryValue boolean dryrun);
}
