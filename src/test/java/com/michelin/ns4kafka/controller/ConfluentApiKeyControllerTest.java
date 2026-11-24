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
package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.service.ConfluentApiKeyService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyResponse;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfluentApiKeyControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    ConfluentApiKeyService confluentApiKeyService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    ConfluentApiKeyController confluentApiKeyController;

    @Test
    void shouldListConfluentApiKeysForNamespace() {
        Namespace ns = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("team-a")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("sa-123").build())
                .build();

        ApiKeyResponse apiKey = ApiKeyResponse.builder()
                .id("key-1")
                .spec(ApiKeyResponse.Spec.builder()
                        .displayName("team-a-key")
                        .owner(ApiKeyResponse.Owner.builder()
                                .id("sa-123")
                                .kind("ServiceAccount")
                                .build())
                        .build())
                .build();

        when(namespaceService.findByName("team-a")).thenReturn(Optional.of(ns));
        when(confluentApiKeyService.findAllForNamespace(ns)).thenReturn(List.of(apiKey));

        List<ApiKeyResponse> actual = confluentApiKeyController.list("team-a");

        assertEquals(1, actual.size());
        assertEquals("key-1", actual.getFirst().id());
    }
}
