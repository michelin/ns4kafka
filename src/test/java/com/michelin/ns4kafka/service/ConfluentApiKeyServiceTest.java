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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyResponse;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

@ExtendWith(MockitoExtension.class)
class ConfluentApiKeyServiceTest {
    @Mock
    ConfluentCloudClient confluentCloudClient;

    @InjectMocks
    ConfluentApiKeyService confluentApiKeyService;

    @Test
    void shouldListApiKeysOwnedByNamespaceKafkaUser() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("team-a")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("sa-123").build())
                .build();

        ApiKeyResponse owned = ApiKeyResponse.builder()
                .id("key-owned")
                .spec(ApiKeyResponse.Spec.builder()
                        .owner(ApiKeyResponse.Owner.builder()
                                .id("sa-123")
                                .kind("ServiceAccount")
                                .build())
                        .build())
                .build();

        ApiKeyResponse anotherOwner = ApiKeyResponse.builder()
                .id("key-other")
                .spec(ApiKeyResponse.Spec.builder()
                        .owner(ApiKeyResponse.Owner.builder()
                                .id("sa-999")
                                .kind("ServiceAccount")
                                .build())
                        .build())
                .build();

        when(confluentCloudClient.listApiKeys("local", "sa-123")).thenReturn(Flux.just(owned, anotherOwner));

        List<ApiKeyResponse> actual = confluentApiKeyService.findAllForNamespace(namespace);

        assertEquals(1, actual.size());
        assertEquals("key-owned", actual.getFirst().id());
    }
}
