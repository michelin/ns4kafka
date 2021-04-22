package com.michelin.ns4kafka.cli.client;

import io.micronaut.http.client.annotation.Client;

@Client("${api.server}/api/namespaces/")
public interface ResourceClient {}
