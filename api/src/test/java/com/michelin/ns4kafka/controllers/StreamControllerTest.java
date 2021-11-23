package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.KafkaStream;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.StreamService;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class StreamControllerTest {

    @Mock
    NamespaceService namespaceService;
    @Mock
    StreamService streamService;
    @Mock
    ApplicationEventPublisher applicationEventPublisher;
    @Mock
    SecurityService securityService;
    @InjectMocks
    StreamController streamController;

    @Test
    void listEmptyStreams() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(streamService.findAllForNamespace(ns))
                .thenReturn(List.of());

        List<KafkaStream> actual = streamController.list("test");
        Assertions.assertEquals(0, actual.size());
    }

    @Test
    void listStreams() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();
        KafkaStream stream2 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream2")
                      .build())
            .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(streamService.findAllForNamespace(ns))
                .thenReturn(List.of(stream1, stream2));

        List<KafkaStream> actual = streamController.list("test");
        Assertions.assertEquals(2, actual.size());
        Assertions.assertTrue(actual.contains(stream1));
        Assertions.assertTrue(actual.contains(stream2));
    }

    @Test
    void getEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        when(streamService.findByName(ns, "test_stream1"))
                .thenReturn(Optional.empty());

        Optional<KafkaStream> actual = streamController.get("test", "test_stream1");
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void getStreamFinded() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        when(streamService.findByName(ns, "test_stream1"))
                .thenReturn(Optional.of(stream1));

        Optional<KafkaStream> actual = streamController.get("test", "test_stream1");
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals(stream1, actual.get());
    }

    @Test
    void createStreamSuccess() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        Mockito.when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
                .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
                .thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        Mockito.when(streamService.create(stream1))
                .thenReturn(stream1);

        var response = streamController.apply("test", stream1, false);
        KafkaStream actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void createStreamSuccessDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        Mockito.when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
                .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
                .thenReturn(Optional.empty());

        var response = streamController.apply("test", stream1, true);
        KafkaStream actual = response.body();
        Mockito.verify(streamService, never()).create(any());
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void updateStreamUnchanged() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        Mockito.when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
                .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
                .thenReturn(Optional.of(stream1));

        var response = streamController.apply("test", stream1, false);
        KafkaStream actual = response.body();
        Mockito.verify(streamService, never()).create(any());
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("test_stream1", actual.getMetadata().getName());
    }

    @Test
    void createStreamValidationError() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        Mockito.when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
                .thenReturn(false);
        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class, () -> streamController.apply("test", stream1, false));
        Mockito.verify(streamService, never()).create(any());
    }

    @Test
    void deleteStreamSuccess() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        Mockito.when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
                .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
                .thenReturn(Optional.of(stream1));

        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        doNothing().when(streamService).delete(stream1);
        var response = streamController.delete("test", "test_stream1", false);
        Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void deleteStreamSuccessDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        Mockito.when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
                .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
                .thenReturn(Optional.of(stream1));

        var response = streamController.delete("test", "test_stream1", true);
        Mockito.verify(streamService, never()).delete(any());
        Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }

    @Test
    void deleteStreamNotFound() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .build())
            .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        Mockito.when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
                .thenReturn(true);

        when(streamService.findByName(ns, "test_stream1"))
                .thenReturn(Optional.empty());

        var response = streamController.delete("test", "test_stream1", false);
        Mockito.verify(streamService, never()).delete(any());
        Assertions.assertEquals(HttpStatus.NOT_FOUND, response.getStatus());
    }

    @Test
    void deleteStreamNotOwner() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        Mockito.when(streamService.isNamespaceOwnerOfKafkaStream(ns, "test_stream1"))
                .thenReturn(false);

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class, () -> streamController.delete("test", "test_stream1", false));
        Mockito.verify(streamService, never()).delete(any());
    }

}
