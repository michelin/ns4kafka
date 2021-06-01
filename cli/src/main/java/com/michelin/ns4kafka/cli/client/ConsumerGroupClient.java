package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.models.Resource;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;

@Client("${kafkactl.api}/api/namespaces/")
public interface ConsumerGroupClient {

    @Post("{namespace}/consumer-group/{consumerGroupName}/reset{?dryrun}")
    Resource reset(
            @Header("Authorization") String token,
            String namespace,
            String consumerGroupName,
            @Body Resource json,
            @QueryValue boolean dryrun);

}
