package com.michelin.ns4kafka.cli.client;

import io.micronaut.http.client.annotation.Client;

@Client("${ns4kafka.api.url}/api/namespaces/")
public interface ResourceClient {}
