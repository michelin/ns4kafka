/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.service;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyResponse;
import jakarta.inject.Singleton;
import java.util.List;

/** Service to list Confluent API keys for a namespace. */
@Singleton
public class ConfluentApiKeyService {
    private final ConfluentCloudClient confluentCloudClient;

    /**
     * Constructor.
     *
     * @param confluentCloudClient The Confluent Cloud client
     */
    public ConfluentApiKeyService(ConfluentCloudClient confluentCloudClient) {
        this.confluentCloudClient = confluentCloudClient;
    }

    /**
     * List all Confluent API keys owned by the namespace Kafka user.
     *
     * @param namespace The namespace
     * @return The list of API keys
     */
    public List<ApiKeyResponse> findAllForNamespace(Namespace namespace) {
        String kafkaUser = namespace.getSpec().getKafkaUser();

        return confluentCloudClient
                .listApiKeys(namespace.getMetadata().getCluster(), kafkaUser)
                .filter(apiKey -> matchesNamespaceUser(apiKey, kafkaUser))
                .collectList()
                .blockOptional()
                .orElse(List.of());
    }

    private boolean matchesNamespaceUser(ApiKeyResponse apiKey, String kafkaUser) {
        return apiKey.spec() != null
                && apiKey.spec().owner() != null
                && apiKey.spec().owner().id() != null
                && apiKey.spec().owner().id().equals(kafkaUser);
    }
}
