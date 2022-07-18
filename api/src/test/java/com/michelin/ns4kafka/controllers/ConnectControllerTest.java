package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.connector.ChangeConnectorState;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.KafkaConnectService;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.utils.SecurityService;
import io.reactivex.Single;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectControllerTest {
    /**
     * Connector service
     */
    @Mock
    KafkaConnectService kafkaConnectService;

    /**
     * Namespace service
     */
    @Mock
    NamespaceService namespaceService;

    /**
     * App service
     */
    @Mock
    ApplicationEventPublisher applicationEventPublisher;

    /**
     * Security service
     */
    @Mock
    SecurityService securityService;

    /**
     * Connector controller
     */
    @InjectMocks
    ConnectController connectController;


    /**
     * The mocked resource quota service
     */
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.findAllForNamespace(ns))
                .thenReturn(List.of());

        List<Connector> actual = connectController.list("test");
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()));

        List<Connector> actual = connectController.list("test");
        Assertions.assertEquals(2, actual.size());
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.findByName(ns, "missing"))
                .thenReturn(Optional.empty());

        Optional<Connector> actual = connectController.getConnector("test", "missing");
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.findByName(ns, "connect1"))
                .thenReturn(Optional.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build()));

        Optional<Connector> actual = connectController.getConnector("test", "connect1");
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("connect1", actual.get().getMetadata().getName());
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(false);

        connectController.deleteConnector("test", "connect1", false)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Namespace not owner of this connector connect1."));
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        Mockito.when(kafkaConnectService.delete(ns,connector))
                .thenReturn(Single.just(HttpResponse.noContent()));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        connectController.deleteConnector("test", "connect1", false)
                .test()
                .assertNoErrors()
                .assertValue(response -> response.getStatus().equals(HttpStatus.NO_CONTENT));
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);

        connectController.deleteConnector("test", "connect1", true)
                .test()
                .assertNoErrors()
                .assertValue(response -> response.getStatus().equals(HttpStatus.NO_CONTENT));

        verify(kafkaConnectService, never()).delete(any(), any());
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(false);

        connectController.apply("test", connector, false)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Namespace not owner of this connector connect1."));
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.validateLocally(ns, connector))
                .thenReturn(Single.just(List.of("Local Validation Error 1")));

        connectController.apply("test", connector, false)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Local Validation Error 1"));
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.validateLocally(ns, connector))
                .thenReturn(Single.just(List.of()));
        Mockito.when(kafkaConnectService.validateRemotely(ns, connector))
                .thenReturn(Single.just(List.of("Remote Validation Error 1")));

        connectController.apply("test", connector, false)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Remote Validation Error 1"));
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
        when(namespaceService.findByName("test"))
        .thenReturn(Optional.of(ns));
        when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
        .thenReturn(true);
        when(kafkaConnectService.validateLocally(ns, connector))
        .thenReturn(Single.just(List.of()));
        when(kafkaConnectService.validateRemotely(ns, connector))
                .thenReturn(Single.just(List.of()));
        when(resourceQuotaService.validateConnectorQuota(ns)).thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(kafkaConnectService.createOrUpdate(connector))
                .thenReturn(expected);

        connectController.apply("test", connector, false)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "created"))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getStatus().getState().equals(expected.getStatus().getState()));
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
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        when(kafkaConnectService.validateLocally(ns, connector))
                .thenReturn(Single.just(List.of()));
        when(kafkaConnectService.validateRemotely(ns, connector))
                .thenReturn(Single.just(List.of()));
        when(resourceQuotaService.validateConnectorQuota(ns)).thenReturn(List.of("Quota error"));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(kafkaConnectService.createOrUpdate(connector))
                .thenReturn(expected);

        connectController.apply("test", connector, false)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Quota error"));
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.validateLocally(ns, connector))
                .thenReturn(Single.just(List.of()));
        Mockito.when(kafkaConnectService.validateRemotely(ns, connector))
                .thenReturn(Single.just(List.of()));
        Mockito.when(kafkaConnectService.findByName(ns, "connect1"))
                .thenReturn(Optional.of(connector));

        connectController.apply("test", connector, false)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "unchanged"))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getStatus().getState().equals(expected.getStatus().getState()));

        verify(kafkaConnectService,never()).createOrUpdate(ArgumentMatchers.any());
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.validateLocally(ns, connector))
                .thenReturn(Single.just(List.of()));
        Mockito.when(kafkaConnectService.validateRemotely(ns, connector))
                .thenReturn(Single.just(List.of()));
        Mockito.when(kafkaConnectService.findByName(ns, "connect1"))
                .thenReturn(Optional.of(connectorOld));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        Mockito.when(kafkaConnectService.createOrUpdate(connector))
                .thenReturn(expected);

        connectController.apply("test", connector, false)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "changed"))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getStatus().getState().equals(expected.getStatus().getState()));
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.validateLocally(ns, connector))
                .thenReturn(Single.just(List.of()));
        Mockito.when(kafkaConnectService.validateRemotely(ns, connector))
                .thenReturn(Single.just(List.of()));

        connectController.apply("test", connector, true)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "created"));

        verify(kafkaConnectService, never()).createOrUpdate(connector);
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        
        when(kafkaConnectService.listUnsynchronizedConnectors(ns))
                .thenReturn(Single.just(List.of(connector1, connector2)));
        
        when(kafkaConnectService.createOrUpdate(connector1)).thenReturn(connector1);
        when(kafkaConnectService.createOrUpdate(connector2)).thenReturn(connector2);

        connectController.importResources("test", false)
                .test()
                .assertValue(response -> response.stream().anyMatch(c -> c.getMetadata().getName().equals("connect1")))
                .assertValue(response -> response.stream().anyMatch(c -> c.getMetadata().getName().equals("connect2")))
                .assertValue(response -> response.stream().noneMatch(c -> c.getMetadata().getName().equals("connect3")));
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));

        when(kafkaConnectService.listUnsynchronizedConnectors(ns))
                .thenReturn(Single.just(List.of(connector1, connector2)));

        connectController.importResources("test", true)
                .test()
                .assertValue(response -> response.stream().anyMatch(c -> c.getMetadata().getName().equals("connect1")))
                .assertValue(response -> response.stream().anyMatch(c -> c.getMetadata().getName().equals("connect2")))
                .assertValue(response -> response.stream().noneMatch(c -> c.getMetadata().getName().equals("connect3")));

        verify(kafkaConnectService, never()).createOrUpdate(connector1);
        verify(kafkaConnectService, never()).createOrUpdate(connector2);
        verify(kafkaConnectService, never()).createOrUpdate(connector3);
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

        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(false);

        ChangeConnectorState restart = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        connectController.changeState("test", "connect1", restart)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Namespace not owner of this connector connect1."));
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.findByName(ns,"connect1"))
                .thenReturn(Optional.empty());

        ChangeConnectorState restart = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        connectController.changeState("test", "connect1", restart)
                .test()
                .assertValue(response -> response.getStatus().equals(HttpStatus.NOT_FOUND));

        verify(kafkaConnectService,never()).restart(ArgumentMatchers.any(), ArgumentMatchers.any());
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        Mockito.when(kafkaConnectService.restart(ArgumentMatchers.any(),ArgumentMatchers.any()))
                .thenReturn(Single.error(new HttpClientResponseException("Rebalancing", HttpResponse.status(HttpStatus.CONFLICT))));

        ChangeConnectorState restart = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        connectController.changeState("test", "connect1", restart)
                .test()
                .assertValue(response -> response.getBody().isPresent()
                        && !response.getBody().get().getStatus().isSuccess()
                        && response.body().getStatus().getErrorMessage().equals("Rebalancing"));
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        Mockito.when(kafkaConnectService.restart(ArgumentMatchers.any(),ArgumentMatchers.any()))
                .thenReturn(Single.just(HttpResponse.noContent()));

        ChangeConnectorState changeConnectorState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec.builder().action(ChangeConnectorState.ConnectorAction.restart).build())
                .build();

        connectController.changeState("test", "connect1", changeConnectorState)
                .test()
                .assertValue(response -> response.getBody().isPresent() &&
                        response.getBody().get().getStatus().isSuccess())
                .assertValue(response -> HttpStatus.NO_CONTENT.equals(response.body().getStatus().getCode()))
                .assertValue(response -> response.body().getMetadata().getName().equals("connect1"));
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        Mockito.when(kafkaConnectService.pause(ArgumentMatchers.any(),ArgumentMatchers.any()))
                .thenReturn(Single.just(HttpResponse.noContent()));

        ChangeConnectorState changeConnectorState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec
                        .builder()
                        .action(ChangeConnectorState.ConnectorAction.pause)
                        .build())
                .build();

        connectController.changeState("test", "connect1", changeConnectorState)
                .test()
                .assertValue(response -> response.getBody().isPresent() &&
                        response.getBody().get().getStatus().isSuccess())
                .assertValue(response -> HttpStatus.NO_CONTENT.equals(response.body().getStatus().getCode()))
                .assertValue(response -> response.body().getMetadata().getName().equals("connect1"));
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
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect(ns, "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.findByName(ns,"connect1"))
                .thenReturn(Optional.of(connector));
        Mockito.when(kafkaConnectService.resume(ArgumentMatchers.any(),ArgumentMatchers.any()))
                .thenReturn(Single.just(HttpResponse.noContent()));

        ChangeConnectorState changeConnectorState = ChangeConnectorState.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(ChangeConnectorState.ChangeConnectorStateSpec
                        .builder()
                        .action(ChangeConnectorState.ConnectorAction.resume)
                        .build())
                .build();

        connectController.changeState("test", "connect1", changeConnectorState)
                .test()
                .assertValue(response -> response.getBody().isPresent() &&
                        response.getBody().get().getStatus().isSuccess())
                .assertValue(response -> HttpStatus.NO_CONTENT.equals(response.body().getStatus().getCode()))
                .assertValue(response -> response.body().getMetadata().getName().equals("connect1"));
    }
}
