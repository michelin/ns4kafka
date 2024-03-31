package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
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
    void applyCreateInvalid() {
        Namespace toCreate = Namespace.builder()
            .metadata(Metadata.builder()
                .name("new-namespace")
                .cluster("local")
                .build())
            .build();
        when(namespaceService.findByName("new-namespace"))
            .thenReturn(Optional.empty());
        when(namespaceService.validateCreation(toCreate))
            .thenReturn(List.of("OneError"));

        ResourceValidationException actual =
            assertThrows(ResourceValidationException.class, () -> namespaceController.apply(toCreate, false));
        assertEquals(1, actual.getValidationErrors().size());
    }

    @Test
    void applyCreateSuccess() {
        Namespace toCreate = Namespace.builder()
            .metadata(Metadata.builder()
                .name("new-namespace")
                .cluster("local")
                .build())
            .build();
        when(namespaceService.findByName("new-namespace"))
            .thenReturn(Optional.empty());
        when(namespaceService.validateCreation(toCreate))
            .thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(namespaceService.createOrUpdate(toCreate))
            .thenReturn(toCreate);

        var response = namespaceController.apply(toCreate, false);
        Namespace actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("new-namespace", actual.getMetadata().getName());
        assertEquals("local", actual.getMetadata().getCluster());
    }

    @Test
    void applyCreateDryRun() {
        Namespace toCreate = Namespace.builder()
            .metadata(Metadata.builder()
                .name("new-namespace")
                .cluster("local")
                .build())
            .build();
        when(namespaceService.findByName("new-namespace"))
            .thenReturn(Optional.empty());
        when(namespaceService.validateCreation(toCreate))
            .thenReturn(List.of());

        var response = namespaceController.apply(toCreate, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(namespaceService, never()).createOrUpdate(toCreate);
    }

    @Test
    void applyUpdateInvalid() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();
        Namespace toUpdate = Namespace.builder()
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
            () -> namespaceController.apply(toUpdate, false));
        assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertIterableEquals(
            List.of("Invalid value \"local\" for field \"cluster\": value is immutable."),
            actual.getValidationErrors());
    }

    @Test
    void applyUpdateSuccess() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();
        Namespace toUpdate = Namespace.builder()
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
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(namespaceService.createOrUpdate(toUpdate))
            .thenReturn(toUpdate);

        var response = namespaceController.apply(toUpdate, false);
        Namespace actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        Assertions.assertNotNull(actual);
        assertEquals("namespace", actual.getMetadata().getName());
        assertEquals("namespace", actual.getMetadata().getNamespace());
        assertEquals("namespace", actual.getMetadata().getName());
        assertEquals("local", actual.getMetadata().getCluster());
        assertEquals("label", actual.getMetadata().getLabels().get("new"));
    }

    @Test
    void applyUpdateSuccess_AlreadyExists() {
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
        Namespace toUpdate = Namespace.builder()
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

        var response = namespaceController.apply(toUpdate, false);
        Namespace actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals(existing, actual);
        verify(namespaceService, never()).createOrUpdate(ArgumentMatchers.any());
    }

    @Test
    void applyUpdateDryRun() {
        Namespace existing = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user")
                .build())
            .build();
        Namespace toUpdate = Namespace.builder()
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

        var response = namespaceController.apply(toUpdate, true);
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        verify(namespaceService, never()).createOrUpdate(toUpdate);
    }

    @Test
    void deleteSuccess() {
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
        when(namespaceService.listAllNamespaceResources(existing))
            .thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        var result = namespaceController.delete("namespace", false);
        assertEquals(HttpResponse.noContent().getStatus(), result.getStatus());

    }

    @Test
    void deleteSuccessDryRun() {
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
        when(namespaceService.listAllNamespaceResources(existing))
            .thenReturn(List.of());

        var result = namespaceController.delete("namespace", true);

        verify(namespaceService, never()).delete(any());
        assertEquals(HttpResponse.noContent().getStatus(), result.getStatus());

    }

    @Test
    void deleteFailNoNamespace() {
        when(namespaceService.findByName("namespace"))
            .thenReturn(Optional.empty());
        var result = namespaceController.delete("namespace", false);
        verify(namespaceService, never()).delete(any());
        assertEquals(HttpResponse.notFound().getStatus(), result.getStatus());

    }

    @Test
    void deleteFailNamespaceNotEmpty() {
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
        when(namespaceService.listAllNamespaceResources(existing))
            .thenReturn(List.of("Topic/topic1"));
        assertThrows(ResourceValidationException.class, () -> namespaceController.delete("namespace", false));
        verify(namespaceService, never()).delete(any());
    }
}
