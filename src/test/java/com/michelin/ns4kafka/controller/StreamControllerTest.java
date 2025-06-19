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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    StreamService streamService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    StreamController streamController;

    @Test
    void shouldListStreamsWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(streamService.findByWildcardName(ns, "*")).thenReturn(List.of());

        List<KafkaStream> actual = streamController.list("test", "*");
        assertEquals(0, actual.size());
    }

    @Test
    void shouldListStreamsWithWildcardParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream2").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(streamService.findByWildcardName(ns, "*")).thenReturn(List.of(stream1, stream2));

        List<KafkaStream> actual = streamController.list("test", "*");
        assertEquals(2, actual.size());
        assertTrue(actual.contains(stream1));
        assertTrue(actual.contains(stream2));
    }

    @Test
    void shouldListStreamsWithNamedParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("prefix.s1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(streamService.findByWildcardName(ns, "prefix.s1")).thenReturn(List.of(stream1));

        List<KafkaStream> actual = streamController.list("test", "prefix.s1");

        assertEquals(1, actual.size());
        assertEquals("prefix.s1", actual.getFirst().getMetadata().getName());
    }

    @Test
    void shouldListStreamsWithEmptyNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("prefix.s1").build())
                .build();

        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder().name("prefix.s2").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(streamService.findByWildcardName(ns, "")).thenReturn(List.of(stream1, stream2));

        List<KafkaStream> actual = streamController.list("test", "");

        assertEquals(2, actual.size());
        assertEquals("prefix.s1", actual.get(0).getMetadata().getName());
        assertEquals("prefix.s2", actual.get(1).getMetadata().getName());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldGetStreamsWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.findByName(ns, "test_stream1")).thenReturn(Optional.empty());

        Optional<KafkaStream> actual = streamController.get("test", "test_stream1");
        assertTrue(actual.isEmpty());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldGetStreams() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.findByName(ns, "test_stream1")).thenReturn(Optional.of(stream1));

        Optional<KafkaStream> actual = streamController.get("test", "test_stream1");
        assertTrue(actual.isPresent());
        assertEquals(stream1, actual.get());
    }

    @Test
    void shouldCreateStreams() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.findByName(ns, "test_stream1")).thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        when(streamService.create(stream1)).thenReturn(stream1);

        var response = streamController.apply("test", stream1, false);
        KafkaStream actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void shouldCreateStreamsInDryRunMode() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.findByName(ns, "test_stream1")).thenReturn(Optional.empty());

        var response = streamController.apply("test", stream1, true);
        KafkaStream actual = response.body();
        verify(streamService, never()).create(any());
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void shouldUpdateStreamsUnchanged() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.findByName(ns, "test_stream1")).thenReturn(Optional.of(stream1));

        var response = streamController.apply("test", stream1, false);
        KafkaStream actual = response.body();
        verify(streamService, never()).create(any());
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void shouldNotCreateStreamsWhenValidationErrors() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(false);

        assertThrows(ResourceValidationException.class, () -> streamController.apply("test", stream1, false));
        verify(streamService, never()).create(any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteStreams() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.findByName(ns, "test_stream1")).thenReturn(Optional.of(stream));

        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        doNothing().when(streamService).delete(ns, stream);
        var response = streamController.delete("test", "test_stream1", false);
        assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteStreamsInDryRunMode() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.findByName(ns, "test_stream1")).thenReturn(Optional.of(stream1));

        var response = streamController.delete("test", "test_stream1", true);
        verify(streamService, never()).delete(any(), any());
        assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteStreamsWhenNotFound() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.findByName(ns, "test_stream1")).thenReturn(Optional.empty());

        var response = streamController.delete("test", "test_stream1", false);
        verify(streamService, never()).delete(any(), any());

        assertEquals(HttpStatus.NOT_FOUND, response.getStatus());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteStreamsWhenNotOwner() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(false);

        assertThrows(ResourceValidationException.class, () -> streamController.delete("test", "test_stream1", false));
        verify(streamService, never()).delete(any(), any());
    }

    @Test
    void shouldBulkDeleteStreams() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream2").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream2")).thenReturn(true);

        when(streamService.findByWildcardName(ns, "test_stream*")).thenReturn(List.of(stream1, stream2));

        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        doNothing().when(streamService).delete(ns, stream1);
        doNothing().when(streamService).delete(ns, stream2);
        var response = streamController.bulkDelete("test", "test_stream*", false);
        assertEquals(HttpStatus.OK, response.getStatus());
    }

    @Test
    void shouldNotBulkDeleteStreamsInDryRunMode() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream2").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream2")).thenReturn(true);

        when(streamService.findByWildcardName(ns, "test_stream*")).thenReturn(List.of(stream1, stream2));

        var response = streamController.bulkDelete("test", "test_stream*", true);
        verify(streamService, never()).delete(any(), any());
        assertEquals(HttpStatus.OK, response.getStatus());
    }

    @Test
    void shouldNotBulkDeleteStreamsWhenNotFound() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.findByWildcardName(ns, "test_stream*")).thenReturn(List.of());

        var response = streamController.bulkDelete("test", "test_stream*", false);
        verify(streamService, never()).delete(any(), any());

        assertEquals(HttpStatus.NOT_FOUND, response.getStatus());
    }

    @Test
    void shouldNotBulkDeleteStreamsWhenNotOwner() throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream1").build())
                .build();

        KafkaStream stream2 = KafkaStream.builder()
                .metadata(Metadata.builder().name("test_stream2").build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1")).thenReturn(true);

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream2")).thenReturn(false);

        when(streamService.findByWildcardName(ns, "test_stream*")).thenReturn(List.of(stream1, stream2));

        assertThrows(
                ResourceValidationException.class, () -> streamController.bulkDelete("test", "test_stream*", false));
        verify(streamService, never()).delete(any(), any());
    }
}
