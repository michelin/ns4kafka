package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controller.connect.ConnectClusterController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.model.connect.cluster.VaultResponse;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.ConnectClusterService;
import com.michelin.ns4kafka.service.ConnectorService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Connect cluster controller test.
 */
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

    @Test
    void shouldListConnectClustersWhenEmpty() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(connectClusterService.findByWildcardNameWithOwnerPermission(ns, "*"))
            .thenReturn(List.of());

        assertTrue(connectClusterController.list("test", "*").isEmpty());
    }

    @Test
    void shouldListConnectClusters() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        List<ConnectCluster> ccs = List.of(
            ConnectCluster.builder().metadata(Metadata.builder().name("connect-cluster").build()).build(),
            ConnectCluster.builder().metadata(Metadata.builder().name("connect-cluster2").build()).build()
        );

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.findByWildcardNameWithOwnerPermission(ns, "*")).thenReturn(ccs);

        assertEquals(ccs, connectClusterController.list("test", "*"));
    }

    @Test
    void shouldListConnectClusterWithNameParameter() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        List<ConnectCluster> ccs = List.of(
            ConnectCluster.builder().metadata(Metadata.builder().name("connect-cluster").build()).build()
        );

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.findByWildcardNameWithOwnerPermission(ns, "connect-cluster")).thenReturn(ccs);

        assertEquals(ccs, connectClusterController.list("test", "connect-cluster"));
    }

    @Test
    void shouldGetConnectClusterWhenEmpty() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(connectClusterService.findByNameWithOwnerPermission(ns, "missing"))
            .thenReturn(Optional.empty());

        Optional<ConnectCluster> actual = connectClusterController.get("test", "missing");
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldGetConnectCluster() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(connectClusterService.findByNameWithOwnerPermission(ns, "connect-cluster"))
            .thenReturn(Optional.of(
                ConnectCluster.builder()
                    .metadata(Metadata.builder()
                        .name("connect-cluster")
                        .build())
                    .build()));

        Optional<ConnectCluster> actual = connectClusterController.get("test", "connect-cluster");
        assertTrue(actual.isPresent());
        assertEquals("connect-cluster", actual.get().getMetadata().getName());
    }

    @Test
    void shouldNotDeleteConnectClusterWhenNotOwner() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
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

    @Test
    void shouldNotDeleteConnectClusterWhenNotFound() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
            .thenReturn(true);
        when(connectClusterService.findByNameWithOwnerPermission(ns, "connect-cluster"))
            .thenReturn(Optional.empty());

        HttpResponse<Void> actual = connectClusterController.delete("test", "connect-cluster", false);
        assertEquals(HttpStatus.NOT_FOUND, actual.getStatus());
    }

    @Test
    void shouldDeleteConnectCluster() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
            .thenReturn(true);
        when(connectorService.findAllByConnectCluster(ns, "connect-cluster"))
            .thenReturn(List.of());
        when(connectClusterService.findByNameWithOwnerPermission(ns, "connect-cluster"))
            .thenReturn(Optional.of(connectCluster));
        doNothing().when(connectClusterService).delete(connectCluster);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> actual = connectClusterController.delete("test", "connect-cluster", false);
        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
    }

    @Test
    void shouldDeleteConnectClusterInDryRunMode() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
            .thenReturn(true);
        when(connectorService.findAllByConnectCluster(ns, "connect-cluster"))
            .thenReturn(List.of());
        when(connectClusterService.findByNameWithOwnerPermission(ns, "connect-cluster"))
            .thenReturn(Optional.of(connectCluster));

        HttpResponse<Void> actual = connectClusterController.delete("test", "connect-cluster", true);
        assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());

        verify(connectClusterService, never()).delete(any());
    }

    @Test
    void shouldNotDeleteConnectClusterWithConnectorsAssociated() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        Connector connector = Connector.builder()
            .metadata(Metadata.builder()
                .name("connect1")
                .build())
            .build();

        when(namespaceService.findByName("test"))
            .thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster"))
            .thenReturn(true);
        when(connectorService.findAllByConnectCluster(ns, "connect-cluster"))
            .thenReturn(List.of(connector));

        ResourceValidationException result = assertThrows(ResourceValidationException.class,
            () -> connectClusterController.delete("test", "connect-cluster", false));

        assertEquals(1, result.getValidationErrors().size());
        assertEquals(
            "Invalid \"delete\" operation: The Kafka Connect \"connect-cluster\" has 1 deployed connector(s): "
                + "connect1. Please remove the associated connector(s) before deleting it.",
            result.getValidationErrors().getFirst());
    }

    @Test
    void shouldCreateConnectCluster() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));
        when(connectClusterService.findByNameWithOwnerPermission(ns, "connect-cluster"))
            .thenReturn(Optional.empty());
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

    @Test
    void shouldNotCreateConnectClusterWhenNotOwner() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(false);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals(
                    "Invalid value \"connect-cluster\" for field \"name\": namespace is not owner of the resource.",
                    ((ResourceValidationException) error).getValidationErrors().getFirst());
            })
            .verify();
    }

    @Test
    void shouldNotCreateConnectClusterWhenValidationReturnErrors() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(
            Mono.just(List.of("Error occurred")));

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Error occurred", ((ResourceValidationException) error).getValidationErrors().getFirst());
            })
            .verify();
    }

    @Test
    void shouldUpdateConnectClusterWhenUnchanged() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));
        when(connectClusterService.findByNameWithOwnerPermission(ns, "connect-cluster")).thenReturn(
            Optional.of(connectCluster));

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
            .consumeNextWith(response -> {
                assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
                assertEquals(connectCluster, response.body());
            })
            .verifyComplete();

        verify(connectClusterService, never()).create(ArgumentMatchers.any());
    }

    @Test
    void shouldUpdateConnectClusterWhenChanged() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://after")
                .build())
            .build();

        ConnectCluster connectClusterChanged = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .url("https://before")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));
        when(connectClusterService.findByNameWithOwnerPermission(ns, "connect-cluster")).thenReturn(
            Optional.of(connectClusterChanged));
        when(connectClusterService.create(connectCluster)).thenReturn(connectCluster);

        StepVerifier.create(connectClusterController.apply("test", connectCluster, false))
            .consumeNextWith(response -> {
                assertEquals("changed", response.header("X-Ns4kafka-Result"));
                assertNotNull(response.body());
                assertEquals("connect-cluster", response.body().getMetadata().getName());
            })
            .verifyComplete();
    }

    @Test
    void shouldCreateConnectClusterInDryRunMode() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceOwnerOfConnectCluster(ns, "connect-cluster")).thenReturn(true);
        when(connectClusterService.validateConnectClusterCreation(connectCluster)).thenReturn(Mono.just(List.of()));
        when(connectClusterService.findByNameWithOwnerPermission(ns, "connect-cluster"))
            .thenReturn(Optional.empty());

        StepVerifier.create(connectClusterController.apply("test", connectCluster, true))
            .consumeNextWith(response -> assertEquals("created", response.header("X-Ns4kafka-Result")))
            .verifyComplete();

        verify(connectClusterService, never()).create(connectCluster);
    }

    @Test
    void shouldListVaultConnectClusterReturnEmptyWhenNoAes256Config() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.findAllForNamespaceWithWritePermission(ns)).thenReturn(List.of(connectCluster));

        List<ConnectCluster> actual = connectClusterController.listVaults("test");
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldListVaultConnectClusterWhenAes256Config() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .build())
            .build();

        ConnectCluster connectClusterAes256 = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster-aes256")
                .build())
            .spec(ConnectCluster.ConnectClusterSpec.builder()
                .aes256Key("myKeyEncryption")
                .aes256Salt("p8t42EhY9z2eSUdpGeq7HX7RboMrsJAhUnu3EEJJVS")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.findAllForNamespaceWithWritePermission(ns)).thenReturn(
            List.of(connectCluster, connectClusterAes256));

        List<ConnectCluster> actual = connectClusterController.listVaults("test");
        assertEquals(1, actual.size());
    }

    @Test
    void shouldNotVaultOnNotAllowedConnectCluster() {
        String connectClusterName = "connect-cluster-na";
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
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
        assertEquals("Invalid value \"connect-cluster-na\" for field \"connect-cluster\": "
                + "namespace is not allowed to use this Kafka Connect.",
            result.getValidationErrors().getFirst());
    }

    @Test
    void shouldNotVaultOnConnectClusterWithInvalidAes256Config() {
        String connectClusterName = "connect-cluster-aes256";
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .topicValidator(TopicValidator.makeDefault())
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectClusterService.isNamespaceAllowedForConnectCluster(ns, connectClusterName)).thenReturn(true);
        when(connectClusterService.validateConnectClusterVault(ns, connectClusterName)).thenReturn(
            List.of("Error config."));

        var secrets = List.of("secret");
        ResourceValidationException result = assertThrows(ResourceValidationException.class,
            () -> connectClusterController.vaultPassword("test", connectClusterName, secrets));
        assertEquals(1, result.getValidationErrors().size());
        assertEquals("Error config.", result.getValidationErrors().getFirst());
    }

    @Test
    void shouldVaultOnConnectClusterWithValidAes256Config() {
        String connectClusterName = "connect-cluster-aes256";
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
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

        final List<VaultResponse> actual =
            connectClusterController.vaultPassword("test", connectClusterName, List.of("secret"));
        assertEquals("secret", actual.getFirst().getSpec().getClearText());
        assertEquals("encryptedSecret", actual.getFirst().getSpec().getEncrypted());
    }
}
