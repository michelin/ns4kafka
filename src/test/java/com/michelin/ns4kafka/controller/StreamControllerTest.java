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
import com.michelin.ns4kafka.model.query.KafkaStreamFilterParams;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Optional;
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
    void listEmptyStreams() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();
        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(streamService.findAllForNamespace(ns, KafkaStreamFilterParams.builder().name(List.of("*")).build()))
            .thenReturn(List.of());

        List<KafkaStream> actual = streamController.list("test", Optional.empty());
        assertEquals(0, actual.size());
    }

    @Test
    void listStreamsWithoutParameter() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .build())
            .build();

        KafkaStream stream2 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream2")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(streamService.findAllForNamespace(ns, KafkaStreamFilterParams.builder().name(List.of("*")).build()))
            .thenReturn(List.of(stream1, stream2));

        List<KafkaStream> actual = streamController.list("test", Optional.empty());
        assertEquals(2, actual.size());
        assertTrue(actual.contains(stream1));
        assertTrue(actual.contains(stream2));
    }

    @Test
    void listStreamsWithNameParameters() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder().metadata(Metadata.builder().name("prefix.s1").build()).build();
        KafkaStream stream2 = KafkaStream.builder().metadata(Metadata.builder().name("prefix.s2").build()).build();

        List<String> nameParamsList = List.of("prefix.s1", "prefix.s2");
        Optional<List<String>> nameParams = Optional.of(nameParamsList);
        KafkaStreamFilterParams searchParams = KafkaStreamFilterParams.builder().name(nameParamsList).build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(streamService.findAllForNamespace(ns, searchParams))
            .thenReturn(List.of(stream1, stream2));

        List<KafkaStream> actual = streamController.list("test", nameParams);

        assertEquals(2, actual.size());
        assertEquals("prefix.s1", actual.get(0).getMetadata().getName());
        assertEquals("prefix.s2", actual.get(1).getMetadata().getName());
    }

    @Test
    void listStreamsWithEmptyNameParameter() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder().metadata(Metadata.builder().name("prefix.s1").build()).build();
        KafkaStream stream2 = KafkaStream.builder().metadata(Metadata.builder().name("prefix.s2").build()).build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(streamService.findAllForNamespace(ns, KafkaStreamFilterParams.builder().name(List.of("")).build()))
                .thenReturn(List.of(stream1, stream2));

        List<KafkaStream> actual = streamController.list("test", Optional.of(List.of("")));

        assertEquals(2, actual.size());
        assertEquals("prefix.s1", actual.get(0).getMetadata().getName());
        assertEquals("prefix.s2", actual.get(1).getMetadata().getName());
    }

    @Test
    void getEmpty() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.findByName(ns, "test_stream1"))
            .thenReturn(Optional.empty());

        Optional<KafkaStream> actual = streamController.get("test", "test_stream1");
        assertTrue(actual.isEmpty());
    }

    @Test
    void getStreamFound() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.findByName(ns, "test_stream1"))
            .thenReturn(Optional.of(stream1));

        Optional<KafkaStream> actual = streamController.get("test", "test_stream1");
        assertTrue(actual.isPresent());
        assertEquals(stream1, actual.get());
    }

    @Test
    void createStreamSuccess() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
            .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
            .thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        when(streamService.create(stream1))
            .thenReturn(stream1);

        var response = streamController.apply("test", stream1, false);
        KafkaStream actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void createStreamSuccessDryRun() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
            .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
            .thenReturn(Optional.empty());

        var response = streamController.apply("test", stream1, true);
        KafkaStream actual = response.body();
        verify(streamService, never()).create(any());
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void updateStreamUnchanged() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
            .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
            .thenReturn(Optional.of(stream1));

        var response = streamController.apply("test", stream1, false);
        KafkaStream actual = response.body();
        verify(streamService, never()).create(any());
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void createStreamValidationError() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
            .thenReturn(false);

        assertThrows(ResourceValidationException.class, () -> streamController.apply("test", stream1, false));
        verify(streamService, never()).create(any());
    }

    @Test
    void deleteStreamSuccess() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
            .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
            .thenReturn(Optional.of(stream1));

        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        doNothing().when(streamService).delete(ns, stream1);
        var response = streamController.delete("test", "test_stream1", false);
        assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void deleteStreamSuccessDryRun() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
            .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
            .thenReturn(Optional.of(stream1));

        var response = streamController.delete("test", "test_stream1", true);
        verify(streamService, never()).delete(any(), any());
        assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void deleteStreamNotFound() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
            .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
            .thenReturn(Optional.empty());

        var response = streamController.delete("test", "test_stream1", false);
        verify(streamService, never()).delete(any(), any());

        assertEquals(HttpStatus.NOT_FOUND, response.getStatus());
    }

    @Test
    void deleteStreamNotOwner() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));

        when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
            .thenReturn(false);

        assertThrows(ResourceValidationException.class, () -> streamController.delete("test", "test_stream1", false));
        verify(streamService, never()).delete(any(), any());
    }
}
