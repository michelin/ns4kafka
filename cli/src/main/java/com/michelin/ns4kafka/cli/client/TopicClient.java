package com.michelin.ns4kafka.cli.client;

import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.client.annotation.Client;

@Client("${kafkactl.api}/api/namespaces/")
public interface TopicClient {

    @Post("{namespace}/topic/{topic}/empty{?dryrun}")
    void empty(@Header("Authorization") String token, String namespace, String topic, @QueryValue boolean dryrun);

}
