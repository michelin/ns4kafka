package com.michelin.ns4kafka.cli.client;

import io.micronaut.http.client.annotation.Client;

@Client("${cluster.server}/api/namespaces/")
public interface ResourceClient {}
