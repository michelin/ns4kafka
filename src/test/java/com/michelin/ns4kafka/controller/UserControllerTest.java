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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class UserControllerTest {
    @Mock
    NamespaceService namespaces;

    @Mock
    ConfluentCloudClient client;

    @Mock
    SecurityService security;

    @Mock
    ApplicationEventPublisher<AuditLog> events;

    @InjectMocks
    UserController controller;

    @BeforeEach
    void setUp() {
        Namespace namespace = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("test")
                        .cluster("cloud")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("sa-test").build())
                .build();
        when(namespaces.findByName("test")).thenReturn(Optional.of(namespace));
    }

    @Test
    void shouldRejectAnotherUserBeforeAccessingConfluent() {
        assertThrows(
                ResourceValidationException.class, () -> controller.deleteApiKey("test", "sa-other", "KEY", false));
        verifyNoInteractions(client, events);
    }

    @Test
    void shouldAuditTheDeletedKey() {
        when(client.deleteApiKey("cloud", "sa-test", "KEY", false)).thenReturn(Mono.just(true));
        assertEquals(
                HttpStatus.NO_CONTENT,
                controller.deleteApiKey("test", "sa-test", "KEY", false).getStatus());
        ArgumentCaptor<AuditLog> capture = ArgumentCaptor.forClass(AuditLog.class);
        verify(events).publishEvent(capture.capture());
        assertEquals(ApplyStatus.DELETED, capture.getValue().getOperation());
        assertEquals("KEY", capture.getValue().getMetadata().getName());
    }

    @Test
    void shouldNotAuditMissingKey() {
        when(client.deleteApiKey("cloud", "sa-test", "KEY", false)).thenReturn(Mono.just(false));
        assertEquals(
                HttpStatus.NOT_FOUND,
                controller.deleteApiKey("test", "sa-test", "KEY", false).getStatus());
        verifyNoInteractions(events);
    }

    @Test
    void shouldNotAuditDryRun() {
        when(client.deleteApiKey("cloud", "sa-test", "KEY", true)).thenReturn(Mono.just(true));
        assertEquals(
                HttpStatus.NO_CONTENT,
                controller.deleteApiKey("test", "sa-test", "KEY", true).getStatus());
        verifyNoInteractions(events);
    }
}
