package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.quota.ResourceQuotaController;
import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ResourceQuotaControllerTest {
    @InjectMocks
    ResourceQuotaController resourceQuotaController;

    @Mock
    ResourceQuotaService resourceQuotaService;

    @Mock
    NamespaceService namespaceService;

    @Mock
    SecurityService securityService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    /**
     * Validate quota listing
     */
    @Test
    void list() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ResourceQuotaResponse response = ResourceQuotaResponse.builder()
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic("0/INF")
                        .countPartition("0/INF")
                        .countConnector("0/INF")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.findByNamespace(ns.getMetadata().getName())).thenReturn(Optional.empty());
        when(resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns, Optional.empty())).thenReturn(response);

        List<ResourceQuotaResponse> actual = resourceQuotaController.list("test");
        assertEquals(1, actual.size());
        assertEquals(response, actual.get(0));
    }

    /**
     * Validate quota get is empty
     */
    @Test
    void getEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.findByName(ns.getMetadata().getName(), "quotaName")).thenReturn(Optional.empty());

        Optional<ResourceQuotaResponse> actual = resourceQuotaController.get("test", "quotaName");
        assertTrue(actual.isEmpty());
    }

    /**
     * Validate quota get present
     */
    @Test
    void getPresent() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        ResourceQuotaResponse response = ResourceQuotaResponse.builder()
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic("0/INF")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.findByName(ns.getMetadata().getName(), "quotaName")).thenReturn(Optional.of(resourceQuota));
        when(resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns, Optional.of(resourceQuota))).thenReturn(response);

        Optional<ResourceQuotaResponse> actual = resourceQuotaController.get("test", "quotaName");
        assertTrue(actual.isPresent());
        assertEquals(response, actual.get());
    }

    /**
     * Validate quota apply when there are validation errors
     */
    @Test
    void applyValidationErrors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of("Quota already exceeded"));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
                () -> resourceQuotaController.apply("test", resourceQuota, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Quota already exceeded"), actual.getValidationErrors());

        verify(resourceQuotaService, never()).create(ArgumentMatchers.any());
    }

    /**
     * Validate quota apply when quota is unchanged
     */
    @Test
    void applyUnchanged() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of());
        when(resourceQuotaService.findByNamespace(ns.getMetadata().getName())).thenReturn(Optional.of(resourceQuota));

        var response = resourceQuotaController.apply("test", resourceQuota, false);
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        verify(resourceQuotaService, never()).create(ArgumentMatchers.any());
        assertEquals(resourceQuota, response.body());
    }

    /**
     * Validate quota apply in dry mode
     */
    @Test
    void applyDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of());
        when(resourceQuotaService.findByNamespace(ns.getMetadata().getName())).thenReturn(Optional.empty());

        var response = resourceQuotaController.apply("test", resourceQuota, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(resourceQuotaService, never()).create(ArgumentMatchers.any());
    }

    /**
     * Validate quota apply when quota is created
     */
    @Test
    void applyCreated() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("created-quota")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of());
        when(resourceQuotaService.findByNamespace(ns.getMetadata().getName())).thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(resourceQuotaService.create(resourceQuota)).thenReturn(resourceQuota);

        var response = resourceQuotaController.apply("test", resourceQuota, false);
        ResourceQuota actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("created-quota", actual.getMetadata().getName());
    }

    /**
     * Validate quota apply when quota is updated
     */
    @Test
    void applyUpdated() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ResourceQuota resourceQuotaExisting = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("created-quota")
                        .build())
                .spec(Map.of("count/topics", "3"))
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("created-quota")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of());
        when(resourceQuotaService.findByNamespace(ns.getMetadata().getName())).thenReturn(Optional.of(resourceQuotaExisting));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(resourceQuotaService.create(resourceQuota)).thenReturn(resourceQuota);

        var response = resourceQuotaController.apply("test", resourceQuota, false);
        ResourceQuota actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("created-quota", actual.getMetadata().getName());
        assertEquals("1", actual.getSpec().get("count/topics"));
    }

    /**
     * Validate resource quota deletion when quota is not found
     */
    @Test
    void deleteNotFound() {
        when(resourceQuotaService.findByName("test", "quota")).thenReturn(Optional.empty());
        HttpResponse<Void> actual = resourceQuotaController.delete("test", "quota", false);
        assertEquals(HttpStatus.NOT_FOUND, actual.getStatus());
        verify(resourceQuotaService, never()).delete(ArgumentMatchers.any());
    }

    /**
     * Validate resource quota deletion in dry run mode
     */
    @Test
    void deleteDryRun() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("created-quota")
                        .build())
                .spec(Map.of("count/topics", "3"))
                .build();

        when(resourceQuotaService.findByName("test", "quota")).thenReturn(Optional.of(resourceQuota));
        HttpResponse<Void> actual = resourceQuotaController.delete("test", "quota", true);
        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
        verify(resourceQuotaService, never()).delete(ArgumentMatchers.any());
    }

    /**
     * Validate resource quota deletion
     */
    @Test
    void delete() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("created-quota")
                        .build())
                .spec(Map.of("count/topics", "3"))
                .build();

        when(resourceQuotaService.findByName("test", "quota")).thenReturn(Optional.of(resourceQuota));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        doNothing().when(resourceQuotaService).delete(resourceQuota);

        HttpResponse<Void> actual = resourceQuotaController.delete("test", "quota", false);
        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
        verify(resourceQuotaService, times(1)).delete(resourceQuota);
    }
}
