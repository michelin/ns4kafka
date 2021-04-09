package com.michelin.ns4kafka.cli.client;

import io.micronaut.http.client.annotation.Client;

@Client("http://localhost:8080/api/namespaces/")
public interface ResourceClient {}
