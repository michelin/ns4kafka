package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

import java.util.List;

@Client("${kafkactl.api}")
public interface ClusterResourceClient {

    @Post("/login")
    BearerAccessRefreshToken login(@Body UsernameAndPasswordRequest request);

    @Get("/token_info")
    UserInfoResponse tokenInfo(@Header("Authorization") String token);

    @Get("/api-resources")
    List<ApiResource> listResourceDefinitions();

    @Delete("/api/{kind}/{resource}{?dryrun}")
    void delete(@Header("Authorization") String token, String kind, String resource, @QueryValue boolean dryrun);

    @Post("/api/{kind}{?dryrun}")
    Resource apply(@Header("Authorization") String token, String kind, @Body Resource json, @QueryValue boolean dryrun);

    @Get("/api/{kind}")
    List<Resource> list(@Header("Authorization") String token, String kind);

    @Get("/api/{kind}/{resource}")
    Resource get(@Header("Authorization") String token, String kind, String resource);

}
