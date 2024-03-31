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
import io.micronaut.security.utils.SecurityService;
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
    void applySuccess() {
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

        when(namespaceService.findByName(any())).thenReturn(Optional.of(ns));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        var response = roleBindingController.apply("test", rolebinding, false);
        RoleBinding actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals(actual.getMetadata().getName(), rolebinding.getMetadata().getName());
    }

    @Test
    void applySuccess_AlreadyExists() {
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

        when(namespaceService.findByName(any())).thenReturn(Optional.of(ns));
        when(roleBindingService.findByName("test", "test.rolebinding"))
            .thenReturn(Optional.of(rolebinding));

        var response = roleBindingController.apply("test", rolebinding, false);
        RoleBinding actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals(actual.getMetadata().getName(), rolebinding.getMetadata().getName());
        verify(roleBindingService, never()).create(ArgumentMatchers.any());
    }

    @Test
    void applySuccess_Changed() {
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

        when(namespaceService.findByName(any())).thenReturn(Optional.of(ns));
        when(roleBindingService.findByName("test", "test.rolebinding"))
            .thenReturn(Optional.of(rolebindingOld));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        var response = roleBindingController.apply("test", rolebinding, false);
        RoleBinding actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals(actual.getMetadata().getName(), rolebinding.getMetadata().getName());
    }

    @Test
    void createDryRun() {
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

        when(namespaceService.findByName(any())).thenReturn(Optional.of(ns));

        var response = roleBindingController.apply("test", rolebinding, true);
        RoleBinding actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(roleBindingService, never()).create(rolebinding);
    }

    @Test
    void deleteSucess() {

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

        //when(namespaceService.findByName(any())).thenReturn(Optional.of(ns));
        when(roleBindingService.findByName(any(), any())).thenReturn(Optional.of(rolebinding));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        assertDoesNotThrow(
            () -> roleBindingController.delete("test", "test.rolebinding", false)
        );
    }

    @Test
    void deleteSuccessDryRun() {
        RoleBinding rolebinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("test.rolebinding")
                .build())
            .build();

        when(roleBindingService.findByName(any(), any())).thenReturn(Optional.of(rolebinding));

        roleBindingController.delete("test", "test.rolebinding", true);
        verify(roleBindingService, never()).delete(any());
    }
}
