package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micronaut.http.HttpResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class NamespaceControllerTest {
    @Mock
    NamespaceService namespaceService;
    @Mock
    ApplicationEventPublisher applicationEventPublisher;
    @Mock
    SecurityService securityService;

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
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        Mockito.when(namespaceService.createOrUpdate(toCreate))
                .thenReturn(toCreate);

        var response = namespaceController.apply(toCreate, false);
        Namespace actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("new-namespace", actual.getMetadata().getName());
        Assertions.assertEquals("local", actual.getMetadata().getCluster());
    }
    @Test
    void applyCreateDryRun(){
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

        var response = namespaceController.apply(toCreate, true);
        Namespace actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(namespaceService, never()).createOrUpdate(toCreate);
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
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        Mockito.when(namespaceService.createOrUpdate(toUpdate))
                .thenReturn(toUpdate);

        var response = namespaceController.apply(toUpdate, false);
        Namespace actual = response.body();
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("namespace", actual.getMetadata().getName());
        Assertions.assertEquals("local", actual.getMetadata().getCluster());
        Assertions.assertEquals("label", actual.getMetadata().getLabels().get("new"));
    }
    @Test
    void applyUpdateSuccess_AlreadyExists(){
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
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(existing));

        var response = namespaceController.apply(toUpdate, false);
        Namespace actual = response.body();
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals(existing, actual);
        verify(namespaceService,never()).createOrUpdate(ArgumentMatchers.any());
    }
    @Test
    void applyUpdateDryRun(){
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

        var response = namespaceController.apply(toUpdate, true);
        Namespace actual = response.body();
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        verify(namespaceService, never()).createOrUpdate(toUpdate);
    }

    @Test
    void deleteSucess() {
        Namespace existing = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(existing));
        Mockito.when(namespaceService.listAllNamespaceResources(existing))
                .thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        var result = namespaceController.delete("namespace", false);
        Assertions.assertEquals(HttpResponse.noContent().getStatus(), result.getStatus());

    }

    @Test
    void deleteSucessDryRun() {
        Namespace existing = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(existing));
        Mockito.when(namespaceService.listAllNamespaceResources(existing))
                .thenReturn(List.of());

        var result = namespaceController.delete("namespace", true);

        verify(namespaceService, never()).delete(any());
        Assertions.assertEquals(HttpResponse.noContent().getStatus(), result.getStatus());

    }

    @Test
    void deleteFailNoNamespace() {
        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.empty());
        var result = namespaceController.delete("namespace", false);
        verify(namespaceService, never()).delete(any());
        Assertions.assertEquals(HttpResponse.notFound().getStatus(), result.getStatus());

    }

    @Test
    void deleteFailNamespaceNotEmpty() {
        Namespace existing = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(existing));
        Mockito.when(namespaceService.listAllNamespaceResources(existing))
                .thenReturn(List.of("Topic/topic1"));
        Assertions.assertThrows(ResourceValidationException.class,() -> namespaceController.delete("namespace", false));
        verify(namespaceService, never()).delete(any());

    }

}
