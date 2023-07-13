package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.connect.ConnectClusterController;
import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.models.connect.cluster.VaultResponse;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
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
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

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

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.findAllByNamespaceOwner(ns))
                .thenReturn(List.of());

        List<ConnectCluster> actual = connectClusterController.list("test");
        assertTrue(actual.isEmpty());
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

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.findAllByNamespaceOwner(ns))
                .thenReturn(List.of(
                        ConnectCluster.builder()
                                .metadata(ObjectMeta.builder().name("connect-cluster")
                                        .build())
                                .build(),
                        ConnectCluster.builder()
                                .metadata(ObjectMeta.builder().name("connect-cluster2")
                                        .build())
                                .build()));

        List<ConnectCluster> actual = connectClusterController.list("test");
        assertEquals(2, actual.size());
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

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "missing"))
                .thenReturn(Optional.empty());

        Optional<ConnectCluster> actual = connectClusterController.getConnectCluster("test", "missing");
        assertTrue(actual.isEmpty());
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

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "connect-cluster"))
                .thenReturn(Optional.of(
                        ConnectCluster.builder()
                                .metadata(ObjectMeta.builder().name("connect-cluster")
                                        .build())
                                .build()));

        Optional<ConnectCluster> actual = connectClusterController.getConnectCluster("test", "connect-cluster");
        assertTrue(actual.isPresent());
        assertEquals("connect-cluster", actual.get().getMetadata().getName());
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

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(false);

        assertThrows(ResourceValidationException.class,
                () -> connectClusterController.delete("test", "connect-cluster", false));
    }

    /**
     * Test connect cluster deletion when not found
     */
    @Test
    void deleteConnectClusterNotFound() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(true);
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "connect-cluster"))
                .thenReturn(Optional.empty());

        HttpResponse<Void> actual = connectClusterController.delete("test", "connect-cluster", false);
        assertEquals(HttpStatus.NOT_FOUND, actual.getStatus());
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

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(true);
        when(connectorService.findAllByConnectCluster(ns, "connect-cluster"))
                .thenReturn(List.of());
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "connect-cluster"))
                .thenReturn(Optional.of(connectCluster));
        doNothing().when(connectClusterService).delete(connectCluster);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> actual = connectClusterController.delete("test", "connect-cluster", false);
        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
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

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(true);
        when(connectorService.findAllByConnectCluster(ns, "connect-cluster"))
                .thenReturn(List.of());
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "connect-cluster"))
                .thenReturn(Optional.of(connectCluster));

        HttpResponse<Void> actual = connectClusterController.delete("test", "connect-cluster", true);
        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());

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

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
                .thenReturn(true);
        when(connectorService.findAllByConnectCluster(ns, "connect-cluster"))
                .thenReturn(List.of(connector));

        ResourceValidationException result = assertThrows(ResourceValidationException.class,
                () -> connectClusterController.delete("test", "connect-cluster", false));

        assertEquals(1, result.getValidationErrors().size());
        assertEquals("The Connect cluster connect-cluster has 1 deployed connector(s): connect1. Please remove the associated connector(s) before deleting it.", result.getValidationErrors().get(0));
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
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "connect-cluster")).thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        when(connectClusterService.create(connectCluster)).thenReturn(connectCluster);

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
                .consumeNextWith(response -> {
                    assertEquals("created", response.header("X-Ns4kafka-Result"));
                    assertNotNull(response.body());
                    assertEquals("connect-cluster", response.body().getMetadata().getName());
                })
                .verifyComplete();
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
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                    assertEquals("Namespace not owner of this Connect cluster connect-cluster.", ((ResourceValidationException) error).getValidationErrors().get(0));
                })
                .verify();
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
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of("Error occurred")));

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                    assertEquals("Error occurred", ((ResourceValidationException) error).getValidationErrors().get(0));
                })
                .verify();
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
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "connect-cluster")).thenReturn(Optional.of(connectCluster));

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
                .consumeNextWith(response -> {
                    assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
                    assertEquals(connectCluster, response.body());
                })
                .verifyComplete();

        verify(connectClusterService, never()).create(ArgumentMatchers.any());
    }

    /**
     * Validate Connect cluster updated when changed
     */
    @Test
    void updateConnectClusterChanged() {
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
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://after")
                        .build())
                .build();

        ConnectCluster connectClusterChanged = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .url("https://before")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "connect-cluster")).thenReturn(Optional.of(connectClusterChanged));
        when(connectClusterService.create(connectCluster)).thenReturn(connectCluster);

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
                .consumeNextWith(response -> {
                    assertEquals("changed", response.header("X-Ns4kafka-Result"));
                    assertNotNull(response.body());
                    assertEquals("connect-cluster", response.body().getMetadata().getName());
                })
                .verifyComplete();
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
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));
        when(connectClusterService.findByNamespaceAndNameOwner(ns, "connect-cluster")).thenReturn(Optional.empty());

        StepVerifier.create(connectClusterController.apply("test", connectCluster, true))
                .consumeNextWith(response -> assertEquals("created", response.header("X-Ns4kafka-Result")))
                .verifyComplete();

        verify(connectClusterService, never()).create(connectCluster);
    }

    /**
     * List available vault for connect clusters allowed without any vault config.
     */
    @Test
    void listVaultNoConnectClusterAllowedWithAes256Config() {
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
                .spec(ConnectCluster.ConnectClusterSpec.builder().build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.findAllByNamespaceWrite(ns)).thenReturn(List.of(connectCluster));

        List<ConnectCluster> actual = connectClusterController.listVaults("test");
        assertTrue(actual.isEmpty());
    }

    /**
     * List available vault for connect clusters allowed with vault config.
     */
    @Test
    void listVaultConnectClusterAllowedWithAes256Config() {
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
                .spec(ConnectCluster.ConnectClusterSpec.builder().build())
                .build();
        ConnectCluster connectClusterAes256 = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster-aes256")
                        .build())
                .spec(ConnectCluster.ConnectClusterSpec.builder()
                        .aes256Key("myKeyEncryption")
                        .aes256Salt("p8t42EhY9z2eSUdpGeq7HX7RboMrsJAhUnu3EEJJVS")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.findAllByNamespaceWrite(ns)).thenReturn(List.of(connectCluster, connectClusterAes256));

        List<ConnectCluster> actual = connectClusterController.listVaults("test");
        assertEquals(1, actual.size());
    }

    /**
     * Vault password on not allowed kafka connect cluster.
     */
    @Test
    void vaultOnNonAllowedConnectCluster() {
        String connectClusterName = "connect-cluster-na";
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceAllowedForConnectCluster(ns, connectClusterName)).thenReturn(false);
        when(connectClusterService.validateConnectClusterVault(ns, connectClusterName)).thenReturn(List.of());

        var secrets = List.of("secret");
        ResourceValidationException result = assertThrows(ResourceValidationException.class,
                () -> connectClusterController.vaultPassword("test", connectClusterName, secrets));
        assertEquals(1, result.getValidationErrors().size());
        assertEquals("Namespace is not allowed to use this Connect cluster connect-cluster-na.", result.getValidationErrors().get(0));
    }

    /**
     * Vault password on not valid kafka connect cluster aes256 config.
     */
    @Test
    void vaultOnNotValidAES256ConnectCluster() {
        String connectClusterName = "connect-cluster-aes256";
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceAllowedForConnectCluster(ns, connectClusterName)).thenReturn(true);
        when(connectClusterService.validateConnectClusterVault(ns, connectClusterName)).thenReturn(List.of("Error config."));

        var secrets = List.of("secret");
        ResourceValidationException result = assertThrows(ResourceValidationException.class,
                () -> connectClusterController.vaultPassword("test", connectClusterName, secrets));
        assertEquals(1, result.getValidationErrors().size());
        assertEquals("Error config.", result.getValidationErrors().get(0));
    }

    /**
     * Vault password on not valid kafka connect cluster aes256 config.
     */
    @Test
    void vaultOnValidAES256ConnectCluster() {
        String connectClusterName = "connect-cluster-aes256";
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceAllowedForConnectCluster(ns, connectClusterName)).thenReturn(true);
        when(connectClusterService.validateConnectClusterVault(ns, connectClusterName)).thenReturn(List.of());
        when(connectClusterService.vaultPassword(ns, connectClusterName, List.of("secret")))
                .thenReturn(List.of(VaultResponse.builder()
                        .spec(VaultResponse.VaultResponseSpec.builder()
                                .clearText("secret")
                                .encrypted("encryptedSecret")
                                .build())
                        .build()
                ));

        final List<VaultResponse> actual = connectClusterController.vaultPassword("test", connectClusterName, List.of("secret"));
        assertEquals("secret", actual.get(0).getSpec().getClearText());
        assertEquals("encryptedSecret", actual.get(0).getSpec().getEncrypted());
    }
}
