package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NamespaceControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    NamespaceController namespaceController;

    @Test
    void shouldListNamespacesWithWildcardParameter() {
        Namespace ns1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .build())
            .build();

        Namespace ns2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns2")
                .build())
            .build();

        when(namespaceService.findByWildcardName("*"))
            .thenReturn(List.of(ns1, ns2));

        assertEquals(List.of(ns1, ns2), namespaceController.list("*"));
    }

    @Test
    void shouldListNamespacesWithNameParameter() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns")
                .build())
            .build();

        when(namespaceService.findByWildcardName("ns"))
            .thenReturn(List.of(ns));

        assertEquals(List.of(ns), namespaceController.list("ns"));
    }

    @Test
    void shouldListNamespacesWhenEmpty() {
        when(namespaceService.findByWildcardName("*")).thenReturn(List.of());
        assertEquals(List.of(), namespaceController.list("*"));
    }

    @Test
    void shouldNotCreateNamespaceWhenValidationErrors() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("new-namespace")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("new-namespace"))
            .thenReturn(Optional.empty());
        when(namespaceService.validateCreation(namespace))
            .thenReturn(List.of("OneError"));

        ResourceValidationException actual =
            assertThrows(ResourceValidationException.class, () -> namespaceController.apply(namespace, false));
        assertEquals(1, actual.getValidationErrors().size());
    }

    @Test
    void shouldCreateNamespace() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("new-namespace")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("new-namespace"))
            .thenReturn(Optional.empty());
        when(namespaceService.validateCreation(namespace))
            .thenReturn(List.of());
        when(securityService.username())
            .thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN))
            .thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(namespaceService.createOrUpdate(namespace))
            .thenReturn(namespace);

        var response = namespaceController.apply(namespace, false);
        Namespace actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("new-namespace", actual.getMetadata().getName());
        assertEquals("local", actual.getMetadata().getCluster());
    }

    @Test
    void shouldCreateNamespaceInDryRunMode() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("new-namespace")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("new-namespace"))
            .thenReturn(Optional.empty());
        when(namespaceService.validateCreation(namespace))
            .thenReturn(List.of());

        var response = namespaceController.apply(namespace, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(namespaceService, never()).createOrUpdate(namespace);
    }

    @Test
    void shouldNotUpdateNamespaceWhenValidationErrors() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        Namespace namespaceToUpdate = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local-change")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user-change")
                .build())
            .build();

        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.of(existing));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> namespaceController.apply(namespaceToUpdate, false));

        assertEquals(1, actual.getValidationErrors().size());
        assertIterableEquals(
            List.of("Invalid value \"local\" for field \"cluster\": value is immutable."),
            actual.getValidationErrors());
    }

    @Test
    void shouldUpdateNamespace() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        Namespace namespaceToUpdate = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .labels(Map.of("new", "label"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.of(existing));
        when(securityService.username())
            .thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN))
            .thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(namespaceService.createOrUpdate(namespaceToUpdate))
            .thenReturn(namespaceToUpdate);

        var response = namespaceController.apply(namespaceToUpdate, false);
        Namespace actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertNotNull(actual);
        assertEquals("namespace", actual.getMetadata().getName());
        assertEquals("namespace", actual.getMetadata().getNamespace());
        assertEquals("namespace", actual.getMetadata().getName());
        assertEquals("local", actual.getMetadata().getCluster());
        assertEquals("label", actual.getMetadata().getLabels().get("new"));
    }

    @Test
    void shouldNotUpdateNamespaceWhenAlreadyExists() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .namespace("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        Namespace namespaceToUpdate = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.of(existing));

        var response = namespaceController.apply(namespaceToUpdate, false);
        Namespace actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals(existing, actual);
        verify(namespaceService, never()).createOrUpdate(any());
    }

    @Test
    void shouldUpdateNamespaceInDryRunMode() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        Namespace namespaceToUpdate = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .labels(Map.of("new", "label"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.of(existing));

        var response = namespaceController.apply(namespaceToUpdate, true);
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        verify(namespaceService, never()).createOrUpdate(namespaceToUpdate);
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteNamespace() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.of(existing));
        when(namespaceService.findAllResourcesByNamespace(existing))
            .thenReturn(List.of());
        when(securityService.username())
            .thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN))
            .thenReturn(false);

        doNothing().when(applicationEventPublisher).publishEvent(any());
        var result = namespaceController.delete("namespace", false);
        assertEquals(HttpResponse.noContent().getStatus(), result.getStatus());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteNamespaceInDryRunMode() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.of(existing));
        when(namespaceService.findAllResourcesByNamespace(existing))
            .thenReturn(List.of());

        var result = namespaceController.delete("namespace", true);

        verify(namespaceService, never()).delete(any());
        assertEquals(HttpResponse.noContent().getStatus(), result.getStatus());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteNamespaceWhenNotFound() {
        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.empty());

        var result = namespaceController.delete("namespace", false);

        verify(namespaceService, never()).delete(any());
        assertEquals(HttpResponse.notFound().getStatus(), result.getStatus());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteNamespaceWhenResourcesAreStillLinkedWithIt() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.of(existing));
        when(namespaceService.findAllResourcesByNamespace(existing))
            .thenReturn(List.of("Topic/topic1"));

        assertThrows(ResourceValidationException.class,
            () -> namespaceController.delete("namespace", false));
        verify(namespaceService, never()).delete(any());
    }

    @Test
    void shouldDeleteNamespaces() {
        Namespace namespace1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace1")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        Namespace namespace2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace2")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByWildcardName("namespace*"))
            .thenReturn(List.of(namespace1, namespace2));
        when(namespaceService.findAllResourcesByNamespace(namespace1))
            .thenReturn(List.of());
        when(namespaceService.findAllResourcesByNamespace(namespace2))
            .thenReturn(List.of());
        when(securityService.username())
            .thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN))
            .thenReturn(false);

        doNothing().when(applicationEventPublisher).publishEvent(any());
        var result = namespaceController.bulkDelete("namespace*", false);
        assertEquals(HttpResponse.noContent().getStatus(), result.getStatus());
    }

    @Test
    void shouldDeleteNamespacesInDryRunMode() {
        Namespace namespace1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace1")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        Namespace namespace2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace2")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByWildcardName("namespace*"))
            .thenReturn(List.of(namespace1, namespace2));
        when(namespaceService.findAllResourcesByNamespace(namespace1))
            .thenReturn(List.of());
        when(namespaceService.findAllResourcesByNamespace(namespace2))
            .thenReturn(List.of());

        var result = namespaceController.bulkDelete("namespace*", true);
        verify(namespaceService, never()).delete(any());
        assertEquals(HttpResponse.noContent().getStatus(), result.getStatus());
    }

    @Test
    void shouldNotDeleteNamespacesWhenResourcesAreStillLinkedWithIt() {
        Namespace namespace1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace1")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        Namespace namespace2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace2")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();

        when(namespaceService.findByWildcardName("namespace*"))
            .thenReturn(List.of(namespace1, namespace2));
        when(namespaceService.findAllResourcesByNamespace(namespace1))
            .thenReturn(List.of("Topic/topic1"));
        when(namespaceService.findAllResourcesByNamespace(namespace2))
            .thenReturn(List.of());

        assertThrows(ResourceValidationException.class,
            () -> namespaceController.bulkDelete("namespace*", false));
        verify(namespaceService, never()).delete(any());
    }

    @Test
    void shouldNotDeleteNamespacesWhenPatternMatchesNothing() {
        when(namespaceService.findByWildcardName("namespace*")).thenReturn(List.of());
        var result = namespaceController.bulkDelete("namespace*", false);
        assertEquals(HttpResponse.notFound().getStatus(), result.getStatus());
    }
}
