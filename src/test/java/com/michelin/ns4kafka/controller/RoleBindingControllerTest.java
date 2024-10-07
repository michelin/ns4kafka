package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.RoleBindingService;
import io.micronaut.context.event.ApplicationEventPublisher;
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
class RoleBindingControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    RoleBindingService roleBindingService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    RoleBindingController roleBindingController;

    @Test
    void shouldCreateRoleBinding() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        RoleBinding rolebinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("test.rolebinding")
                .build())
            .build();

        when(namespaceService.findByName(any()))
            .thenReturn(Optional.of(ns));
        when(securityService.username())
            .thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN))
            .thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        var response = roleBindingController.apply("test", rolebinding, false);
        RoleBinding actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals(actual.getMetadata().getName(), rolebinding.getMetadata().getName());
    }

    @Test
    void shouldNotCreateRoleBindingWhenAlreadyExists() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        RoleBinding rolebinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("test.rolebinding")
                .build())
            .build();

        when(namespaceService.findByName(any()))
            .thenReturn(Optional.of(ns));
        when(roleBindingService.findByName("test", "test.rolebinding"))
            .thenReturn(Optional.of(rolebinding));

        var response = roleBindingController.apply("test", rolebinding, false);
        RoleBinding actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals(actual.getMetadata().getName(), rolebinding.getMetadata().getName());
        verify(roleBindingService, never()).create(ArgumentMatchers.any());
    }

    @Test
    void shouldChangeRoleBinding() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        RoleBinding rolebinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("test.rolebinding")
                .build())
            .build();

        RoleBinding rolebindingOld = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("test.rolebinding")
                .labels(Map.of("old", "label"))
                .build())
            .build();

        when(namespaceService.findByName(any()))
            .thenReturn(Optional.of(ns));
        when(roleBindingService.findByName("test", "test.rolebinding"))
            .thenReturn(Optional.of(rolebindingOld));
        when(securityService.username())
            .thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN))
            .thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        var response = roleBindingController.apply("test", rolebinding, false);
        RoleBinding actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals(actual.getMetadata().getName(), rolebinding.getMetadata().getName());
    }

    @Test
    void shouldCreateRoleBindingInDryRunMode() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        RoleBinding rolebinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("test.rolebinding")
                .build())
            .build();

        when(namespaceService.findByName(any()))
            .thenReturn(Optional.of(ns));

        var response = roleBindingController.apply("test", rolebinding, true);
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(roleBindingService, never()).create(rolebinding);
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteRoleBinding() {
        RoleBinding rolebinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("test.rolebinding")
                .build())
            .build();

        when(roleBindingService.findByName(any(), any()))
            .thenReturn(Optional.of(rolebinding));
        when(securityService.username())
            .thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN))
            .thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        assertDoesNotThrow(
            () -> roleBindingController.delete("test", "test.rolebinding", false)
        );
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteRoleBindingInDryRunMode() {
        RoleBinding rolebinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("test.rolebinding")
                .build())
            .build();

        when(roleBindingService.findByName(any(), any()))
            .thenReturn(Optional.of(rolebinding));

        roleBindingController.delete("test", "test.rolebinding", true);
        verify(roleBindingService, never()).delete(any());
    }

    @Test
    void shouldDeleteRoleBindings() {
        RoleBinding rolebinding1 = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("test.rolebinding1")
                        .build())
                .build();
        RoleBinding rolebinding2 = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("test.rolebinding2")
                        .build())
                .build();

        when(roleBindingService.findByWildcardName(any(), any()))
                .thenReturn(List.of(rolebinding1, rolebinding2));
        when(securityService.username())
                .thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN))
                .thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        assertDoesNotThrow(
                () -> roleBindingController.bulkDelete("test", "test.rolebinding*", false)
        );
    }

    @Test
    void shouldDeleteRoleBindingsInDryRunMode() {
        RoleBinding rolebinding1 = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("test.rolebinding1")
                        .build())
                .build();
        RoleBinding rolebinding2 = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("test.rolebinding2")
                        .build())
                .build();

        when(roleBindingService.findByWildcardName(any(), any()))
                .thenReturn(List.of(rolebinding1, rolebinding2));

        roleBindingController.bulkDelete("test", "test.rolebinding*", true);
        verify(roleBindingService, never()).delete(any());
    }

    @Test
    void shouldNotDeleteRoleBindingsWhenNotFound() {
        when(roleBindingService.findByWildcardName(any(), any()))
                .thenReturn(List.of());

        var response = roleBindingController.bulkDelete("test", "test.rolebinding*", false);
        verify(roleBindingService, never()).delete(any());
        assertEquals(HttpStatus.NOT_FOUND, response.getStatus());
    }

    @Test
    void shouldListRoleBindingsWithNameParameter() {
        RoleBinding rb1 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("namespace-rb1")
                .build())
            .build();

        when(roleBindingService.findByWildcardName("test", "namespace-rb1"))
            .thenReturn(List.of(rb1));

        assertEquals(List.of(rb1), roleBindingController.list("test", "namespace-rb1"));
    }

    @Test
    void shouldListRoleBindingsWithEmptyNameParameter() {
        RoleBinding rb1 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("namespace-rb1")
                .build())
            .build();

        RoleBinding rb2 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("namespace-rb2")
                .build())
            .build();

        when(roleBindingService.findByWildcardName("test", "*"))
            .thenReturn(List.of(rb1, rb2));
        when(roleBindingService.findByWildcardName("test", ""))
            .thenReturn(List.of(rb1, rb2));

        assertEquals(List.of(rb1, rb2), roleBindingController.list("test", "*"));
        assertEquals(List.of(rb1, rb2), roleBindingController.list("test", ""));
    }
}
