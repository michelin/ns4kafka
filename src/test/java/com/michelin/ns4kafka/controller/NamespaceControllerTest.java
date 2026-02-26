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

import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;
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
import com.michelin.ns4kafka.security.auth.AuthenticationRoleBinding;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.authentication.Authentication;
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
    void shouldListNamespaces() {
        Namespace ns1 = Namespace.builder()
                .metadata(Metadata.builder().name("ns1").build())
                .build();

        Namespace ns2 = Namespace.builder()
                .metadata(Metadata.builder().name("ns2").build())
                .build();

        when(namespaceService.findByWildcardName("*")).thenReturn(List.of(ns1, ns2));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(true);

        assertEquals(List.of(ns1, ns2), namespaceController.list(Map.of()));
    }

    @Test
    void shouldListNamespacesWithWildcardNameParameter() {
        Namespace ns1 = Namespace.builder()
                .metadata(Metadata.builder().name("ns1").build())
                .build();

        Namespace ns2 = Namespace.builder()
                .metadata(Metadata.builder().name("ns2").build())
                .build();

        when(namespaceService.findByWildcardName("*")).thenReturn(List.of(ns1, ns2));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(true);

        assertEquals(List.of(ns1, ns2), namespaceController.list(Map.of("name", "*")));
    }

    @Test
    void shouldListNamespacesWithNameParameter() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("ns").build())
                .build();

        when(namespaceService.findByWildcardName("ns")).thenReturn(List.of(ns));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(true);

        assertEquals(List.of(ns), namespaceController.list(Map.of("name", "ns")));
    }

    @Test
    void shouldListNamespaceFilteredByTopic() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("ns").build())
                .build();

        when(namespaceService.findByWildcardName("*")).thenReturn(List.of(ns));
        when(namespaceService.findByTopicName(List.of(ns), "myTopicName")).thenReturn(Optional.of(ns));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(true);

        assertEquals(List.of(ns), namespaceController.list(Map.of("topic", "myTopicName")));
    }

    @Test
    void shouldListNoNamespaceFilteredByTopic() {
        Namespace ns = Namespace.builder()
                .metadata(Metadata.builder().name("ns").build())
                .build();

        when(namespaceService.findByWildcardName("*")).thenReturn(List.of(ns));
        when(namespaceService.findByTopicName(List.of(ns), "myTopicName")).thenReturn(Optional.empty());
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(true);

        assertEquals(List.of(), namespaceController.list(Map.of("topic", "myTopicName")));
    }

    @Test
    void shouldListNamespacesWhenEmpty() {
        when(namespaceService.findByWildcardName("*")).thenReturn(List.of());
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(true);
        assertEquals(List.of(), namespaceController.list(Map.of()));
    }

    @Test
    void shouldListNamespacesAsNonAdmin() {
        Namespace ns1 = Namespace.builder()
                .metadata(Metadata.builder().name("ns1").build())
                .build();
        Namespace ns2 = Namespace.builder()
                .metadata(Metadata.builder().name("ns2").build())
                .build();
        Namespace ns3 = Namespace.builder()
                .metadata(Metadata.builder().name("ns3").build())
                .build();

        when(namespaceService.findByWildcardName("*")).thenReturn(List.of(ns1, ns2, ns3));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);

        Authentication authentication = Authentication.build(
                "user",
                List.of(),
                Map.of(
                        ROLE_BINDINGS,
                        List.of(AuthenticationRoleBinding.builder()
                                .namespaces(List.of("ns1", "ns2"))
                                .build())));
        when(securityService.getAuthentication()).thenReturn(Optional.of(authentication));

        List<Namespace> result = namespaceController.list(Map.of());
        assertEquals(2, result.size());
        assertIterableEquals(List.of(ns1, ns2), result);
    }

    @Test
    void shouldListNamespacesAsNonAdminWithNoAccess() {
        Namespace ns1 = Namespace.builder()
                .metadata(Metadata.builder().name("ns1").build())
                .build();

        when(namespaceService.findByWildcardName("*")).thenReturn(List.of(ns1));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);

        Authentication authentication = Authentication.build("user", List.of(), Map.of(ROLE_BINDINGS, List.of()));
        when(securityService.getAuthentication()).thenReturn(Optional.of(authentication));

        List<Namespace> result = namespaceController.list(Map.of());
        assertEquals(0, result.size());
    }

    @Test
    void shouldNotCreateNamespaceWhenValidationErrors() {
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("new-namespace")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("new-namespace")).thenReturn(Optional.empty());
        when(namespaceService.validateCreation(namespace)).thenReturn(List.of("OneError"));

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

        when(namespaceService.findByName("new-namespace")).thenReturn(Optional.empty());
        when(namespaceService.validateCreation(namespace)).thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(namespaceService.createOrUpdate(namespace)).thenReturn(namespace);

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

        when(namespaceService.findByName("new-namespace")).thenReturn(Optional.empty());
        when(namespaceService.validateCreation(namespace)).thenReturn(List.of());

        var response = namespaceController.apply(namespace, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(namespaceService, never()).createOrUpdate(namespace);
    }

    @Test
    void shouldNotUpdateNamespaceWhenValidationErrors() {
        Namespace existing = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        Namespace namespaceToUpdate = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("namespace")
                        .cluster("local-change")
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user-change").build())
                .build();

        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(existing));

        ResourceValidationException actual = assertThrows(
                ResourceValidationException.class, () -> namespaceController.apply(namespaceToUpdate, false));

        assertEquals(1, actual.getValidationErrors().size());
        assertIterableEquals(
                List.of("Invalid value \"local\" for field \"cluster\": value is immutable."),
                actual.getValidationErrors());
    }

    @Test
    void shouldUpdateNamespace() {
        Namespace existing = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        Namespace namespaceToUpdate = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .labels(Map.of("new", "label"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(existing));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(namespaceService.createOrUpdate(namespaceToUpdate)).thenReturn(namespaceToUpdate);

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
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        Namespace namespaceToUpdate = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(existing));

        var response = namespaceController.apply(namespaceToUpdate, false);
        Namespace actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals(existing, actual);
        verify(namespaceService, never()).createOrUpdate(any());
    }

    @Test
    void shouldUpdateNamespaceInDryRunMode() {
        Namespace existing = Namespace.builder()
                .metadata(Metadata.builder().name("namespace").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        Namespace namespaceToUpdate = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("namespace")
                        .cluster("local")
                        .labels(Map.of("new", "label"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        when(namespaceService.findByName("namespace")).thenReturn(Optional.of(existing));

        var response = namespaceController.apply(namespaceToUpdate, true);
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        verify(namespaceService, never()).createOrUpdate(namespaceToUpdate);
    }

    @Test
    void shouldDeleteNamespaces() {
        Namespace namespace1 = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        Namespace namespace2 = Namespace.builder()
                .metadata(Metadata.builder().name("namespace2").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        when(namespaceService.findByWildcardName("namespace*")).thenReturn(List.of(namespace1, namespace2));
        when(namespaceService.findAllResourcesByNamespace(namespace1)).thenReturn(List.of());
        when(namespaceService.findAllResourcesByNamespace(namespace2)).thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);

        doNothing().when(applicationEventPublisher).publishEvent(any());
        var result = namespaceController.delete("namespace*", false);
        assertEquals(HttpStatus.OK, result.getStatus());
    }

    @Test
    void shouldNotDeleteNamespacesInDryRunMode() {
        Namespace namespace1 = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        Namespace namespace2 = Namespace.builder()
                .metadata(Metadata.builder().name("namespace2").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        when(namespaceService.findByWildcardName("namespace*")).thenReturn(List.of(namespace1, namespace2));
        when(namespaceService.findAllResourcesByNamespace(namespace1)).thenReturn(List.of());
        when(namespaceService.findAllResourcesByNamespace(namespace2)).thenReturn(List.of());

        var result = namespaceController.delete("namespace*", true);
        verify(namespaceService, never()).delete(any());
        assertEquals(HttpStatus.OK, result.getStatus());
    }

    @Test
    void shouldNoDeleteNamespacesWithResources() {
        Namespace namespace1 = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        Namespace namespace2 = Namespace.builder()
                .metadata(Metadata.builder().name("namespace2").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().kafkaUser("user").build())
                .build();

        when(namespaceService.findByWildcardName("namespace*")).thenReturn(List.of(namespace1, namespace2));
        when(namespaceService.findAllResourcesByNamespace(namespace1)).thenReturn(List.of("Topic/topic1"));
        when(namespaceService.findAllResourcesByNamespace(namespace2)).thenReturn(List.of());

        assertThrows(ResourceValidationException.class, () -> namespaceController.delete("namespace*", false));
        verify(namespaceService, never()).delete(any());
    }

    @Test
    void shouldNotDeleteNamespacesWhenNoPatternMatches() {
        when(namespaceService.findByWildcardName("namespace*")).thenReturn(List.of());
        var result = namespaceController.delete("namespace*", false);
        assertEquals(HttpResponse.notFound().getStatus(), result.getStatus());
    }
}
