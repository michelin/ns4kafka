package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connector.ChangeConnectorState;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.ConnectorService;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectorControllerTest {
    @Mock
    ConnectorService connectorService;

    @Mock
    NamespaceService namespaceService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    ConnectorController connectorController;

    @Mock
    ResourceQuotaService resourceQuotaService;

    /**
     * Test connector listing when namespace is empty
     */
    @Test
    void listEmptyConnectors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.findAllForNamespace(ns))
                .thenReturn(List.of());

        List<Connector> actual = connectorController.list("test");
        Assertions.assertTrue(actual.isEmpty());
    }

    /**
     * Test connector listing
     */
    @Test
    void listMultipleConnectors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()));

        List<Connector> actual = connectorController.list("test");
        assertEquals(2, actual.size());
    }

    /**
     * Test get connector by name when it does not exist
     */
    @Test
    void getConnectorEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.findByName(ns, "missing"))
                .thenReturn(Optional.empty());

        Optional<Connector> actual = connectorController.getConnector("test", "missing");
        Assertions.assertTrue(actual.isEmpty());
    }

    /**
     * Test get connector by name
     */
    @Test
    void getConnector() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.findByName(ns, "connect1"))
                .thenReturn(Optional.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build()));

        Optional<Connector> actual = connectorController.getConnector("test", "connect1");
        Assertions.assertTrue(actual.isPresent());
        assertEquals("connect1", actual.get().getMetadata().getName());
    }

    /**
     * Test connector deletion when namespace is not owner
     */
    @Test
    void deleteConnectorNotOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(false);

        StepVerifier.create(connectorController.deleteConnector("test", "connect1", false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Namespace not owner of this connector connect1.", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();
    }

    /**
     * Test connector deletion when namespace is owner
     */
    @Test
    void deleteConnectorOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        when(connectorService.delete(ns,connector))
                .thenReturn(Mono.just(HttpResponse.noContent()));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        StepVerifier.create(connectorController.deleteConnector("test", "connect1", false))
            .consumeNextWith(response -> assertEquals(HttpStatus.NO_CONTENT, response.getStatus()))
            .verifyComplete();
    }

    /**
     * Test connector deletion in dry run mode
     */
    @Test
    void deleteConnectorOwnedDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);

        StepVerifier.create(connectorController.deleteConnector("test", "connect1", true))
            .consumeNextWith(response -> assertEquals(HttpStatus.NO_CONTENT, response.getStatus()))
            .verifyComplete();

        verify(connectorService, never()).delete(any(), any());
    }

    /**
     * Test connector deletion when connector is not found
     */
    @Test
    void deleteConnectorNotFound() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.findByName(ns,"connect1"))
                .thenReturn(Optional.empty());
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);

        StepVerifier.create(connectorController.deleteConnector("test", "connect1", true))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();

        verify(connectorService, never()).delete(any(), any());
    }

    /**
     * Test connector creation when namespace is not owner
     */
    @Test
    void createConnectorNotOwner() {
        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(false);

        StepVerifier.create(connectorController.apply("test", connector, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Namespace not owner of this connector connect1.", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();
    }

    /**
     * Test connector creation when there are local errors
     */
    @Test
    void createConnectorLocalErrors() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(new HashMap<>()).build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.validateLocally(ns, connector))
                .thenReturn(Mono.just(List.of("Local Validation Error 1")));

        StepVerifier.create(connectorController.apply("test", connector, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Local Validation Error 1", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();
    }

    /**
     * Test connector creation when there are remote errors
     */
    @Test
    void createConnectorRemoteErrors() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(new HashMap<>()).build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.validateLocally(ns, connector))
                .thenReturn(Mono.just(List.of()));
        when(connectorService.validateRemotely(ns, connector))
                .thenReturn(Mono.just(List.of("Remote Validation Error 1")));

        StepVerifier.create(connectorController.apply("test", connector, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Remote Validation Error 1", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();
    }

    /**
     * Test connector creation on success
     */
    @Test
    void createConnectorSuccess() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(new HashMap<>()).build())
                .build();
        Connector expected = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(Map.of("name", "connect1")).build())
                .status(Connector.ConnectorStatus.builder().state(Connector.TaskState.UNASSIGNED).build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1")).thenReturn(true);
        when(connectorService.validateLocally(ns, connector)).thenReturn(Mono.just(List.of()));
        when(connectorService.validateRemotely(ns, connector)).thenReturn(Mono.just(List.of()));
        when(resourceQuotaService.validateConnectorQuota(any())).thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(connectorService.createOrUpdate(connector))
                .thenReturn(expected);

        StepVerifier.create(connectorController.apply("test", connector, false))
            .consumeNextWith(response -> {
                assertEquals("created", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals(expected.getStatus().getState(), response.getBody().get().getStatus().getState());
            })
            .verifyComplete();
    }

    /**
     * Test connector creation when there are validation failures
     */
    @Test
    void createConnectorFailQuotaValidation() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(new HashMap<>()).build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.validateLocally(ns, connector))
                .thenReturn(Mono.just(List.of()));
        when(connectorService.validateRemotely(ns, connector))
                .thenReturn(Mono.just(List.of()));
        when(resourceQuotaService.validateConnectorQuota(ns)).thenReturn(List.of("Quota error"));

        StepVerifier.create(connectorController.apply("test", connector, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Quota error", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();
    }

    /**
     * Test connector creation unchanged
     */
    @Test
    void createConnectorSuccessAlreadyExists() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(new HashMap<>()).build())
                .build();
        Connector expected = Connector.builder()
                .metadata(ObjectMeta.builder()
                        .namespace("test")
                        .cluster("local")
                        .name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(Map.of("name", "connect1")).build())
                .status(Connector.ConnectorStatus.builder().state(Connector.TaskState.UNASSIGNED).build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1")).thenReturn(true);
        when(connectorService.validateLocally(ns, connector)).thenReturn(Mono.just(List.of()));
        when(connectorService.validateRemotely(ns, connector)).thenReturn(Mono.just(List.of()));
        when(connectorService.findByName(ns, "connect1")).thenReturn(Optional.of(connector));

        StepVerifier.create(connectorController.apply("test", connector, false))
            .consumeNextWith(response -> {
                assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals(expected.getStatus().getState(), response.getBody().get().getStatus().getState());
            })
            .verifyComplete();

        verify(connectorService,never()).createOrUpdate(ArgumentMatchers.any());
    }

    /**
     * Test connector change
     */
    @Test
    void createConnectorSuccessChanged() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(new HashMap<>()).build())
                .build();
        Connector connectorOld = Connector.builder().metadata(ObjectMeta.builder().name("connect1").labels(Map.of("label", "labelValue")).build()).build();
        Connector expected = Connector.builder()
                .metadata(ObjectMeta.builder()
                        .name("connect1")
                        .labels(Map.of("label", "labelValue"))
                        .build())
                .spec(Connector.ConnectorSpec.builder().config(Map.of("name", "connect1")).build())
                .status(Connector.ConnectorStatus.builder().state(Connector.TaskState.UNASSIGNED).build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.validateLocally(ns, connector))
                .thenReturn(Mono.just(List.of()));
        when(connectorService.validateRemotely(ns, connector))
                .thenReturn(Mono.just(List.of()));
        when(connectorService.findByName(ns, "connect1"))
                .thenReturn(Optional.of(connectorOld));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(connectorService.createOrUpdate(connector))
                .thenReturn(expected);

        StepVerifier.create(connectorController.apply("test", connector, false))
            .consumeNextWith(response -> {
                assertEquals("changed", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals(expected.getStatus().getState(), response.getBody().get().getStatus().getState());
            })
            .verifyComplete();
    }

    /**
     * Test connector creation in dry mode
     */
    @Test
    void createConnectorDryRun() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder().config(new HashMap<>()).build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.validateLocally(ns, connector))
                .thenReturn(Mono.just(List.of()));
        when(connectorService.validateRemotely(ns, connector))
                .thenReturn(Mono.just(List.of()));

        StepVerifier.create(connectorController.apply("test", connector, true))
            .consumeNextWith(response -> assertEquals("created", response.header("X-Ns4kafka-Result")))
            .verifyComplete();

        verify(connectorService, never()).createOrUpdate(connector);
    }

    /**
     * Test connector import
     */
    @Test
    void importConnector() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Connector connector1 = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        Connector connector2 = Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        
        when(connectorService.listUnsynchronizedConnectors(ns))
                .thenReturn(Flux.fromIterable(List.of(connector1, connector2)));
        
        when(connectorService.createOrUpdate(connector1)).thenReturn(connector1);
        when(connectorService.createOrUpdate(connector2)).thenReturn(connector2);

        StepVerifier.create(connectorController.importResources("test", false))
            .consumeNextWith(connect1 -> assertEquals("connect1", connect1.getMetadata().getName()))
            .consumeNextWith(connect2 -> assertEquals("connect2", connect2.getMetadata().getName()))
            .verifyComplete();
    }

    /**
     * Test connector import in dry mode
     */
    @Test
    void importConnectorDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Connector connector1 = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        Connector connector2 = Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build();
        Connector connector3 = Connector.builder().metadata(ObjectMeta.builder().name("connect3").build()).build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        when(connectorService.listUnsynchronizedConnectors(ns))
                .thenReturn(Flux.fromIterable(List.of(connector1, connector2)));

        StepVerifier.create(connectorController.importResources("test", true))
            .consumeNextWith(connect1 -> assertEquals("connect1", connect1.getMetadata().getName()))
            .consumeNextWith(connect2 -> assertEquals("connect2", connect2.getMetadata().getName()))
            .verifyComplete();

        verify(connectorService, never()).createOrUpdate(connector1);
        verify(connectorService, never()).createOrUpdate(connector2);
        verify(connectorService, never()).createOrUpdate(connector3);
    }

    /**
     * Test connector restart when namespace is not owner
     */
    @Test
    void restartConnectorNotOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(false);

        ChangeConnectorState restart = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        StepVerifier.create(connectorController.changeState("test", "connect1", restart))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Namespace not owner of this connector connect1.", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();
    }

    /**
     * Test connector restart when it does not exist
     */
    @Test
    void restartConnectorNotExists() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.findByName(ns,"connect1"))
                .thenReturn(Optional.empty());

        ChangeConnectorState restart = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        StepVerifier.create(connectorController.changeState("test", "connect1", restart))
            .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
            .verifyComplete();

        verify(connectorService,never()).restart(ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    /**
     * Test connector restart throwing an exception
     */
    @Test
    void restartConnectorException() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        when(connectorService.restart(ArgumentMatchers.any(),ArgumentMatchers.any()))
                .thenReturn(Mono.error(new HttpClientResponseException("Rebalancing", HttpResponse.status(HttpStatus.CONFLICT))));

        ChangeConnectorState restart = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        StepVerifier.create(connectorController.changeState("test", "connect1", restart))
            .consumeNextWith(response -> {
                assertTrue(response.getBody().isPresent());
                assertFalse(response.getBody().get().getStatus().isSuccess());
                assertNotNull(response.body());
                assertEquals("Rebalancing", response.body().getStatus().getErrorMessage());
            })
            .verifyComplete();
    }

    /**
     * Test connector restart when namespace is owner
     */
    @Test
    void restartConnectorOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        when(connectorService.restart(ArgumentMatchers.any(),ArgumentMatchers.any()))
                .thenReturn(Mono.just(HttpResponse.noContent()));

        ChangeConnectorState changeConnectorState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        StepVerifier.create(connectorController.changeState("test", "connect1", changeConnectorState))
            .consumeNextWith(response -> {
                assertTrue(response.getBody().isPresent());
                assertTrue(response.getBody().get().getStatus().isSuccess());
                assertEquals(HttpStatus.NO_CONTENT, response.body().getStatus().getCode());
                assertEquals("connect1", response.body().getMetadata().getName());
            })
            .verifyComplete();
    }

    /**
     * Test connector pause when namespace is owner
     */
    @Test
    void pauseConnectorOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        when(connectorService.pause(ArgumentMatchers.any(),ArgumentMatchers.any()))
                .thenReturn(Mono.just(HttpResponse.noContent()));

        ChangeConnectorState changeConnectorState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec
                        .builder()
                        .action(ChangeConnectorState.ConnectorAction.pause)
                        .build())
                .build();

        StepVerifier.create(connectorController.changeState("test", "connect1", changeConnectorState))
            .consumeNextWith(response -> {
                assertTrue(response.getBody().isPresent());
                assertTrue(response.getBody().get().getStatus().isSuccess());
                assertEquals(HttpStatus.NO_CONTENT, response.body().getStatus().getCode());
                assertEquals("connect1", response.body().getMetadata().getName());
            })
            .verifyComplete();
    }

    /**
     * Test connector resume when namespace is owner
     */
    @Test
    void resumeConnectorOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(connectorService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(connectorService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        when(connectorService.resume(ArgumentMatchers.any(),ArgumentMatchers.any()))
                .thenReturn(Mono.just(HttpResponse.noContent()));

        ChangeConnectorState changeConnectorState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec
                        .builder()
                        .action(ChangeConnectorState.ConnectorAction.resume)
                        .build())
                .build();

        StepVerifier.create(connectorController.changeState("test", "connect1", changeConnectorState))
            .consumeNextWith(response -> {
                assertTrue(response.getBody().isPresent());
                assertTrue(response.getBody().get().getStatus().isSuccess());
                assertEquals(HttpStatus.NO_CONTENT, response.body().getStatus().getCode());
                assertEquals("connect1", response.body().getMetadata().getName());
            })
            .verifyComplete();
    }
}
