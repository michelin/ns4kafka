package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.ConnectCluster;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.ConnectClusterService;
import com.michelin.ns4kafka.services.ConnectorService;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectClusterControllerTest {
    @Mock
    SecurityService securityService;

    @Mock
    NamespaceService namespaceService;

    @Mock
    ConnectClusterService connectClusterService;

    @Mock
    ConnectorService connectorService;

    @InjectMocks
    ConnectClusterController connectClusterController;

    @Mock
    ApplicationEventPublisher applicationEventPublisher;

    /**
     * Test connect clusters listing when namespace is empty
     */
    @Test
    void listEmptyConnectClusters() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(connectClusterService.findAllForNamespace(ns))
                .thenReturn(List.of());

        List<ConnectCluster> actual = connectClusterController.list("test");
        Assertions.assertTrue(actual.isEmpty());
    }

    /**
     * Test connect clusters listing
     */
    @Test
    void listMultipleConnectClusters() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(connectClusterService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        ConnectCluster.builder()
                                .metadata(ObjectMeta.builder().name("connect-cluster")
                                        .build())
                                .build(),
                        ConnectCluster.builder()
                                .metadata(ObjectMeta.builder().name("connect-cluster2")
                                        .build())
                                .build())
                );

        List<ConnectCluster> actual = connectClusterController.list("test");
        Assertions.assertEquals(2, actual.size());
    }

    /**
     * Test get connect cluster by name when it does not exist
     */
    @Test
    void getConnectClusterEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(connectClusterService.findByNamespaceAndName(ns, "missing"))
                .thenReturn(Optional.empty());

        Optional<ConnectCluster> actual = connectClusterController.getConnectCluster("test", "missing");
        Assertions.assertTrue(actual.isEmpty());
    }

    /**
     * Test get connect cluster by name
     */
    @Test
    void getConnectCluster() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(connectClusterService.findByNamespaceAndName(ns, "connect-cluster"))
                .thenReturn(Optional.of(
                        ConnectCluster.builder()
                                .metadata(ObjectMeta.builder().name("connect-cluster")
                                        .build())
                                .build()));

        Optional<ConnectCluster> actual = connectClusterController.getConnectCluster("test", "connect-cluster");
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("connect-cluster", actual.get().getMetadata().getName());
    }

    /**
     * Test connect cluster deletion when namespace is not owner
     */
    @Test
    void deleteConnectClusterNotOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(false);

        Assertions.assertThrows(ResourceValidationException.class,
                () -> connectClusterController.delete("test", "connect-cluster", false));
    }

    /**
     * Test connect cluster deletion when namespace is owner
     */
    @Test
    void deleteConnectClusterOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(true);
        Mockito.when(connectorService.findAllByNamespaceAndConnectCluster(ns,"connect-cluster"))
                .thenReturn(List.of());
        Mockito.when(connectClusterService.findByNamespaceAndName(ns,"connect-cluster"))
                .thenReturn(Optional.of(connectCluster));
        doNothing().when(connectClusterService).delete(connectCluster);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> actual = connectClusterController.delete("test", "connect-cluster", false);
        Assertions.assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
    }

    /**
     * Test connect cluster deletion in dry run mode
     */
    @Test
    void deleteConnectClusterOwnedDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(true);
        Mockito.when(connectorService.findAllByNamespaceAndConnectCluster(ns,"connect-cluster"))
                .thenReturn(List.of());
        Mockito.when(connectClusterService.findByNamespaceAndName(ns,"connect-cluster"))
                .thenReturn(Optional.of(connectCluster));

        HttpResponse<Void> actual = connectClusterController.delete("test", "connect-cluster", true);
        Assertions.assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());

        verify(connectClusterService, never()).delete(any());
    }

    /**
     * Test connect cluster deletion when it has connectors deployed on it
     */
    @Test
    void deleteConnectClusterWithConnectors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(true);
        Mockito.when(connectorService.findAllByNamespaceAndConnectCluster(ns,"connect-cluster"))
                .thenReturn(List.of(connector));

        ResourceValidationException result = Assertions.assertThrows(ResourceValidationException.class,
                () -> connectClusterController.delete("test", "connect-cluster", false));

        Assertions.assertEquals(1, result.getValidationErrors().size());
        Assertions.assertEquals("The Connect cluster connect-cluster has 1 deployed connector(s): connect1. Please remove the associated connector(s) before deleting it.", result.getValidationErrors().get(0));
    }

    /**
     * Validate Connect cluster creation
     */
    @Test
    void createNewConnectCluster() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(List.of());
        when(connectClusterService.findByNamespaceAndName(ns, "connect-cluster")).thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        when(connectClusterService.create(connectCluster)).thenReturn(connectCluster);

        HttpResponse<ConnectCluster> response = connectClusterController.apply("test", connectCluster, false);
        ConnectCluster actual = response.body();

        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("connect-cluster", actual.getMetadata().getName());
    }

    /**
     * Validate Connect cluster creation being not owner
     */
    @Test
    void createNewConnectClusterNotOwner() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(false);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(List.of());

        ResourceValidationException result = Assertions.assertThrows(ResourceValidationException.class,
                () -> connectClusterController.apply("test", connectCluster, false));

        Assertions.assertEquals(1, result.getValidationErrors().size());
        Assertions.assertEquals("Namespace not owner of this Connect cluster connect-cluster.", result.getValidationErrors().get(0));
    }

    /**
     * Validate Connect cluster creation being not owner
     */
    @Test
    void createNewConnectClusterValidationError() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(List.of("Error occurred"));

        ResourceValidationException result = Assertions.assertThrows(ResourceValidationException.class,
                () -> connectClusterController.apply("test", connectCluster, false));

        Assertions.assertEquals(1, result.getValidationErrors().size());
        Assertions.assertEquals("Error occurred", result.getValidationErrors().get(0));
    }

    /**
     * Validate Connect cluster updated when unchanged
     */
    @Test
    void updateConnectClusterUnchanged() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(List.of());
        when(connectClusterService.findByNamespaceAndName(ns, "connect-cluster")).thenReturn(Optional.of(connectCluster));

        HttpResponse<ConnectCluster> response = connectClusterController.apply("test", connectCluster, false);
        ConnectCluster actual = response.body();

        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        verify(connectClusterService, never()).create(ArgumentMatchers.any());
        assertEquals(connectCluster, actual);
    }

    /**
     * Validate Connect cluster creation in dry run mode
     */
    @Test
    void createConnectClusterDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(List.of());
        when(connectClusterService.findByNamespaceAndName(ns, "connect-cluster")).thenReturn(Optional.empty());

        HttpResponse<ConnectCluster> response = connectClusterController.apply("test", connectCluster, true);

        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(connectClusterService, never()).create(connectCluster);
    }
}
