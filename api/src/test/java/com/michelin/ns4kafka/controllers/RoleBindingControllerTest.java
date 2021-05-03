package com.michelin.ns4kafka.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.RoleBindingService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RoleBindingControllerTest {

    @Mock
    NamespaceService namespaceService;
    @Mock
    RoleBindingService roleBindingService;

    @InjectMocks
    RoleBindingController roleBindingController;

    @Test
    public void create() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        RoleBinding rolebinding = RoleBinding.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.rolebinding")
                        .build())
                .build();

        when(namespaceService.findByName(any())).thenReturn(Optional.of(ns));

        RoleBinding actual = roleBindingController.apply("test", rolebinding, false);
        assertEquals(actual.getMetadata().getName(), rolebinding.getMetadata().getName());
    }

    @Test
    public void createDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        RoleBinding rolebinding = RoleBinding.builder()
                .metadata(ObjectMeta.builder()
                        .name("test.rolebinding")
                        .build())
                .build();

        when(namespaceService.findByName(any())).thenReturn(Optional.of(ns));

        RoleBinding actual = roleBindingController.apply("test", rolebinding, true);
        verify(roleBindingService, never()).create(rolebinding);
    }
}
