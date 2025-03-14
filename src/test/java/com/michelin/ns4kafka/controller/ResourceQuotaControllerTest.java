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
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controller.quota.ResourceQuotaController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.model.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.ResourceQuotaService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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

    @Test
    void shouldListQuotaWithoutNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ResourceQuota quota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("test").build())
                .build();

        ResourceQuotaResponse response = ResourceQuotaResponse.builder()
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic("0/INF")
                        .countPartition("0/INF")
                        .countConnector("0/INF")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.findByWildcardName("test", "*")).thenReturn(List.of(quota));
        when(resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns, Optional.of(quota)))
                .thenReturn(response);

        assertEquals(List.of(response), resourceQuotaController.list("test", "*"));
    }

    @Test
    void shouldListQuotaWithNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ResourceQuota quota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("quotaName").build())
                .build();

        ResourceQuotaResponse response = ResourceQuotaResponse.builder()
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic("0/INF")
                        .countPartition("0/INF")
                        .countConnector("0/INF")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.findByWildcardName("test", "quotaName")).thenReturn(List.of(quota));
        when(resourceQuotaService.findByWildcardName("test", "not-found")).thenReturn(List.of());
        when(resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns, Optional.of(quota)))
                .thenReturn(response);

        assertEquals(List.of(response), resourceQuotaController.list("test", "quotaName"));
        assertTrue(resourceQuotaController.list("test", "not-found").isEmpty());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldGetQuotaWhenEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        when(resourceQuotaService.findByName(ns.getMetadata().getName(), "quotaName"))
                .thenReturn(Optional.empty());

        Optional<ResourceQuotaResponse> actual = resourceQuotaController.get("test", "quotaName");
        assertTrue(actual.isEmpty());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldGetQuotaWhenPresent() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("test").build())
                .spec(Map.of("count/topics", "1"))
                .build();

        ResourceQuotaResponse response = ResourceQuotaResponse.builder()
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic("0/INF")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.findByName(ns.getMetadata().getName(), "quotaName"))
                .thenReturn(Optional.of(resourceQuota));
        when(resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns, Optional.of(resourceQuota)))
                .thenReturn(response);

        Optional<ResourceQuotaResponse> actual = resourceQuotaController.get("test", "quotaName");
        assertTrue(actual.isPresent());
        assertEquals(response, actual.get());
    }

    @Test
    void shouldApplyValidationErrors() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("test").build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota))
                .thenReturn(List.of("Quota already exceeded"));

        ResourceValidationException actual = assertThrows(
                ResourceValidationException.class, () -> resourceQuotaController.apply("test", resourceQuota, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertLinesMatch(List.of("Quota already exceeded"), actual.getValidationErrors());

        verify(resourceQuotaService, never()).create(ArgumentMatchers.any());
    }

    @Test
    void shouldApplyUnchanged() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("test").build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of());
        when(resourceQuotaService.findForNamespace(ns.getMetadata().getName())).thenReturn(Optional.of(resourceQuota));

        var response = resourceQuotaController.apply("test", resourceQuota, false);
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        verify(resourceQuotaService, never()).create(ArgumentMatchers.any());
        assertEquals(resourceQuota, response.body());
    }

    @Test
    void shouldApplyDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("test").build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of());
        when(resourceQuotaService.findForNamespace(ns.getMetadata().getName())).thenReturn(Optional.empty());

        var response = resourceQuotaController.apply("test", resourceQuota, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(resourceQuotaService, never()).create(ArgumentMatchers.any());
    }

    @Test
    void shouldApplyCreated() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder()
                        .cluster("local")
                        .name("created-quota")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of());
        when(resourceQuotaService.findForNamespace(ns.getMetadata().getName())).thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(resourceQuotaService.create(resourceQuota)).thenReturn(resourceQuota);

        var response = resourceQuotaController.apply("test", resourceQuota, false);
        ResourceQuota actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("created-quota", actual.getMetadata().getName());
    }

    @Test
    void shouldApplyUpdated() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("test").cluster("local").build())
                .build();

        ResourceQuota resourceQuotaExisting = ResourceQuota.builder()
                .metadata(Metadata.builder()
                        .cluster("local")
                        .name("created-quota")
                        .build())
                .spec(Map.of("count/topics", "3"))
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder()
                        .cluster("local")
                        .name("created-quota")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(resourceQuotaService.validateNewResourceQuota(ns, resourceQuota)).thenReturn(List.of());
        when(resourceQuotaService.findForNamespace(ns.getMetadata().getName()))
                .thenReturn(Optional.of(resourceQuotaExisting));
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

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteQuotaWhenNotFound() {
        when(resourceQuotaService.findByName("test", "quota")).thenReturn(Optional.empty());
        HttpResponse<Void> actual = resourceQuotaController.delete("test", "quota", false);
        assertEquals(HttpStatus.NOT_FOUND, actual.getStatus());
        verify(resourceQuotaService, never()).delete(ArgumentMatchers.any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteQuotaWhenDryRun() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("quota").build())
                .spec(Map.of("count/topics", "3"))
                .build();

        when(resourceQuotaService.findByName("test", "quota")).thenReturn(Optional.of(resourceQuota));
        HttpResponse<Void> actual = resourceQuotaController.delete("test", "quota", true);
        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
        verify(resourceQuotaService, never()).delete(ArgumentMatchers.any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteQuota() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("quota").build())
                .spec(Map.of("count/topics", "3"))
                .build();

        when(resourceQuotaService.findByName("test", "quota")).thenReturn(Optional.of(resourceQuota));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        doNothing().when(resourceQuotaService).delete(resourceQuota);

        HttpResponse<Void> actual = resourceQuotaController.delete("test", "quota", false);
        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
        verify(resourceQuotaService).delete(resourceQuota);
    }

    @Test
    void shouldNotBulkDeleteQuotaWhenNotFound() {
        when(resourceQuotaService.findByWildcardName("test", "quota*")).thenReturn(List.of());
        var actual = resourceQuotaController.bulkDelete("test", "quota*", false);
        assertEquals(HttpStatus.NOT_FOUND, actual.getStatus());
        verify(resourceQuotaService, never()).delete(ArgumentMatchers.any());
    }

    @Test
    void shouldNotBulkDeleteQuotaInDryRunMode() {
        ResourceQuota resourceQuota1 = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("quota1").build())
                .spec(Map.of("count/topics", "3"))
                .build();

        when(resourceQuotaService.findByWildcardName("test", "quota*")).thenReturn(List.of(resourceQuota1));
        var actual = resourceQuotaController.bulkDelete("test", "quota*", true);
        assertEquals(HttpStatus.OK, actual.getStatus());
        verify(resourceQuotaService, never()).delete(ArgumentMatchers.any());
    }

    @Test
    void shouldBulkDeleteQuota() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(Metadata.builder().cluster("local").name("quota").build())
                .spec(Map.of("count/topics", "3"))
                .build();

        when(resourceQuotaService.findByWildcardName("test", "quota*")).thenReturn(List.of(resourceQuota));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        doNothing().when(resourceQuotaService).delete(resourceQuota);

        var actual = resourceQuotaController.bulkDelete("test", "quota*", false);
        assertEquals(HttpStatus.OK, actual.getStatus());
        verify(resourceQuotaService).delete(resourceQuota);
    }
}
