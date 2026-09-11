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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.KafkaUserApiKey;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyRequest;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyResponse;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.security.utils.SecurityService;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UserControllerTest {
    @Mock
    ApplicationContext context;

    @Mock
    NamespaceService namespaces;

    @Mock
    SecurityService security;

    @Mock
    ApplicationEventPublisher<AuditLog> events;

    @Mock
    ConfluentCloudClient client;

    UserController controller;

    @BeforeEach
    void setUp() {
        controller = new UserController(context, namespaces, security, events, client);
        when(namespaces.findByName("ns"))
                .thenReturn(Optional.of(Namespace.builder()
                        .metadata(Resource.Metadata.builder()
                                .name("ns")
                                .cluster("cloud")
                                .build())
                        .spec(Namespace.NamespaceSpec.builder()
                                .kafkaUser("sa-test")
                                .build())
                        .build()));
    }

    @Test
    void shouldReturnCreatedCredentialsWithoutAuditingThem() throws Exception {
        var credentials = new ApiKeyResponse(
                "NEWKEY",
                new ApiKeyResponse.Spec(
                        "secret",
                        new ApiKeyRequest.Owner("sa-test"),
                        new ApiKeyRequest.Resource("lkc-test", "env-test")));
        when(client.createApiKey("cloud", "sa-test")).thenReturn(credentials);
        when(security.username()).thenReturn(Optional.of("caller"));

        var response = controller.createApiKey("ns", "sa-test");

        assertEquals(HttpStatus.CREATED, response.getStatus());
        assertEquals("no-store", response.getHeaders().get("Cache-Control"));
        assertEquals("NEWKEY", response.body().getSpec().getApiKey());
        assertEquals("secret", response.body().getSpec().getApiSecret());
        var json = io.micronaut.json.JsonMapper.createDefault().writeValueAsString(response.body());
        assertEquals(
                "secret",
                io.micronaut.json.JsonMapper.createDefault()
                        .readValue(json, KafkaUserApiKey.class)
                        .getSpec()
                        .getApiSecret());
        assertEquals("sa-test", response.body().getMetadata().getName());
        assertEquals("cloud", response.body().getMetadata().getCluster());
        assertEquals("ns", response.body().getMetadata().getNamespace());
        var capture = ArgumentCaptor.forClass(AuditLog.class);
        verify(events).publishEvent(capture.capture());
        assertNull(capture.getValue().getBefore());
        assertNull(capture.getValue().getAfter());
        assertEquals("caller", capture.getValue().getUser());
        assertEquals(ApplyStatus.CREATED, capture.getValue().getOperation());
    }

    @Test
    void shouldRejectAnotherUserBeforeCallingConfluent() {
        assertThrows(ResourceValidationException.class, () -> controller.createApiKey("ns", "sa-other"));
        verifyNoInteractions(context, client, events);
    }

    @Test
    void shouldNotAuditFailedCreationAndUseExistingStatusEnvelope() throws Exception {
        when(client.createApiKey("cloud", "sa-test"))
                .thenThrow(new HttpStatusException(
                        HttpStatus.GATEWAY_TIMEOUT, "Creation timed out; existing keys are unchanged."));
        var exception = assertThrows(HttpStatusException.class, () -> controller.createApiKey("ns", "sa-test"));
        var error = new ExceptionHandlerController().error(HttpRequest.POST("/", ""), exception);
        assertEquals(HttpStatus.GATEWAY_TIMEOUT, error.getStatus());
        assertEquals(
                "Creation timed out; existing keys are unchanged.",
                error.body().getDetails().getCauses().getFirst());
        verifyNoInteractions(events);
    }
}
