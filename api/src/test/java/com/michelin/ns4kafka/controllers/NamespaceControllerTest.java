package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.NamespaceService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
public class NamespaceControllerTest {
    @Mock
    NamespaceService namespaceService;

    @InjectMocks
    NamespaceController namespaceController;

    @Test
    void applyCreateInvalid(){
        Namespace toCreate = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("new-namespace")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("new-namespace"))
                .thenReturn(Optional.empty());
        Mockito.when(namespaceService.validateCreation(toCreate))
                .thenReturn(List.of("OneError"));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,()->namespaceController.apply(toCreate, false));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
    }
    @Test
    void applyCreateSuccess(){
        Namespace toCreate = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("new-namespace")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("new-namespace"))
                .thenReturn(Optional.empty());
        Mockito.when(namespaceService.validateCreation(toCreate))
                .thenReturn(List.of());
        Mockito.when(namespaceService.createOrUpdate(toCreate))
                .thenReturn(toCreate);

        Namespace actual = namespaceController.apply(toCreate, false);
        Assertions.assertEquals("new-namespace", actual.getMetadata().getName());
        Assertions.assertEquals("local", actual.getMetadata().getCluster());
    }
    @Test
    void applyUpdateInvalid(){
        Namespace existing = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .build())
                .build();
        Namespace toUpdate = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local-change")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user-change")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(existing));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> namespaceController.apply(toUpdate, false));
        Assertions.assertEquals(2, actual.getValidationErrors().size());
        Assertions.assertIterableEquals(
                List.of("Invalid value local-change for cluster: Value is immutable (local)",
                        "Invalid value user-change for kafkaUser: Value is immutable (user)"),
                actual.getValidationErrors());
    }
    @Test
    void applyUpdateSuccess(){
        Namespace existing = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .build())
                .build();
        Namespace toUpdate = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .labels(Map.of("new", "label"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(existing));
        Mockito.when(namespaceService.createOrUpdate(toUpdate))
                .thenReturn(toUpdate);

        Namespace actual = namespaceController.apply(toUpdate, false);
        Assertions.assertEquals("namespace", actual.getMetadata().getName());
        Assertions.assertEquals("local", actual.getMetadata().getCluster());
        Assertions.assertEquals("label", actual.getMetadata().getLabels().get("new"));
    }

}
