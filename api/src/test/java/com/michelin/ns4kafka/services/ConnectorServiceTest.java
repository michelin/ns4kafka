package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.connect.ConnectorClientProxy;
import com.michelin.ns4kafka.services.connect.client.ConnectorClient;
import com.michelin.ns4kafka.services.connect.client.entities.*;
import com.michelin.ns4kafka.services.executors.ConnectorAsyncExecutor;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.ResourceValidator;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectorServiceTest {
    /**
     * Mocked ACL service
     */
    @Mock
    AccessControlEntryService accessControlEntryService;

    /**
     * Mocked Kafka connector client
     */
    @Mock
    ConnectorClient connectorClient;

    /**
     * Mocked connector repository
     */
    @Mock
    ConnectorRepository connectorRepository;

    /**
     * Mocked application context
     */
    @Mock
    ApplicationContext applicationContext;

    /**
     * Mocked kafka connect service
     */
    @InjectMocks
    ConnectorService connectorService;

    /**
     * Test to find all connectors by namespace when there is no connector
     */
    @Test
    void findByNamespaceNone() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        List<Connector> actual = connectorService.findAllForNamespace(ns);

        Assertions.assertTrue(actual.isEmpty());
    }

    /**
     * Test to find all connectors by namespace
     */
    @Test
    void findByNamespaceMultiple() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Connector c1 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect1").build())
                .build();
        Connector c2 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect2").build())
                .build();
        Connector c3 = Connector.builder()
                .metadata(ObjectMeta.builder().name("other-connect1").build())
                .build();
        Connector c4 = Connector.builder()
                .metadata(ObjectMeta.builder().name("other-connect2").build())
                .build();
        Connector c5 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns2-connect1").build())
                .build();

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("other-connect1")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("ns-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.READ)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns2-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.WRITE)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns3-")
                                        .build())
                                .build()
                ));

        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1, c2, c3, c4, c5));

        List<Connector> actual = connectorService.findAllForNamespace(ns);

        Assertions.assertEquals(3, actual.size());
        // contains
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("other-connect1")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("other-connect2")));
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns2-connect1")));
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns3-connect1")));
    }

    /**
     * Test to find a given connector that does not exist
     */
    @Test
    void findByNameNotFound() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        Optional<Connector> actual = connectorService.findByName(ns, "ns-connect1");

        Assertions.assertTrue(actual.isEmpty());
    }

    /**
     * Test to find a given connector
     */
    @Test
    void findByNameFound() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();
        Connector c1 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect1").build())
                .build();
        Connector c2 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect2").build())
                .build();
        Connector c3 = Connector.builder()
                .metadata(ObjectMeta.builder().name("other-connect1").build())
                .build();
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("other-connect1")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("ns-")
                                        .build())
                                .build()
                ));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of(AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("namespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resource("ns-")
                                .build())
                        .build()));
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1, c2, c3));

        Optional<Connector> actual = connectorService.findByName(ns, "ns-connect1");

        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("ns-connect1", actual.get().getMetadata().getName());
    }

    /**
     * Test to validate the configuration of a connector when the KConnect cluster is invalid
     */
    @Test
    void validateLocallyInvalidConnectCluster() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("wrong")
                        .config(Map.of("connector.class", "Test"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .validationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .sinkValidationConstraints(Map.of())
                                .classValidationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        connectorService.validateLocally(ns, connector)
                .test()
                .assertValue(response -> response.size() == 1)
                .assertValue(response -> response.get(0).equals("Invalid value wrong for spec.connectCluster: Value must be one of [local-name]"));
    }

    /**
     * Test to validate the configuration of a connector when the class name is missing
     */
    @Test
    void validateLocallyNoClassName() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of())
                        .build())
                .build();
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        connectorService.validateLocally(ns, connector)
                .test()
                .assertValue(response -> response.size() == 1)
                .assertValue(response -> response.get(0).equals("Invalid value for spec.config.'connector.class': Value must be non-null"));
    }

    /**
     * Test to validate the configuration of a connector when the class name is invalid
     */
    @Test
    void validateLocallyInvalidClassName() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();
        Mockito.when(connectorClient.connectPlugins(ConnectorClientProxy.PROXY_SECRET, "local", "local-name"))
                .thenReturn(Single.just(List.of()));

        connectorService.validateLocally(ns, connector)
                .test()
                .assertValue(response -> response.size() == 1)
                .assertValue(response -> response.get(0).equals("Failed to find any class that implements Connector and which name matches org.apache.kafka.connect.file.FileStreamSinkConnector"));
    }

    /**
     * Test to validate the configuration of a connector when a field should not be null
     */
    @Test
    void validateLocallyValidationErrors() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .validationConstraints(Map.of("missing.field", new ResourceValidator.NonEmptyString()))
                                .sinkValidationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .classValidationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();
        Mockito.when(connectorClient.connectPlugins(ConnectorClientProxy.PROXY_SECRET, "local", "local-name"))
                .thenReturn(Single.just(List.of(new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1"))));

        connectorService.validateLocally(ns, connector)
                .test()
                .assertValue(response -> response.size() == 1)
                .assertValue(response -> response.get(0).equals("Invalid value null for configuration missing.field: Value must be non-null"));
    }

    /**
     * Test to validate the configuration of a connector
     */
    @Test
    void validateLocallySuccess() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectValidator(ConnectValidator.builder()
                                .classValidationConstraints(Map.of())
                                .sinkValidationConstraints(Map.of())
                                .sourceValidationConstraints(Map.of())
                                .validationConstraints(Map.of())
                                .build())
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();
        Mockito.when(connectorClient.connectPlugins(ConnectorClientProxy.PROXY_SECRET, "local", "local-name"))
                .thenReturn(Single.just(List.of(new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1"))));

        connectorService.validateLocally(ns, connector)
                .test()
                .assertValue(List::isEmpty);
    }

    /**
     * Test to invalidate the configuration of a connector against the KConnect cluster
     */
    @Test
    void validateRemotelyErrors() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "com.michelin.NoClass"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ConfigInfos configInfos = new ConfigInfos("name", 1, List.of(),
                List.of(new ConfigInfo(new ConfigKeyInfo(null, null, false, null, null, null, null, 0, null, null, null),
                        new ConfigValueInfo(null, null, null, List.of("error_message"), true))));

        Mockito.when(connectorClient.validate(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq("local"),
                ArgumentMatchers.eq("local-name"),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()))
                .thenReturn(Single.just(configInfos));

        connectorService.validateRemotely(ns, connector)
                .test()
                .assertValue(response -> response.size() == 1)
                .assertValue(response -> response.contains("error_message"));
    }

    /**
     * Test to validate the configuration of a connector against the KConnect cluster
     */
    @Test
    void validateRemotelySuccess() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("local-name")
                        .config(Map.of("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector"))
                        .build())
                .build();

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ConfigInfos configInfos = new ConfigInfos("name", 1, List.of(), List.of());
        Mockito.when(connectorClient.validate(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq("local"),
                ArgumentMatchers.eq("local-name"),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()))
                .thenReturn(Single.just(configInfos));

        connectorService.validateRemotely(ns, connector)
                .test()
                .assertValue(List::isEmpty);
    }

    /**
     * Test the listing of unsynchronized connectors when they are all unsynchronized
     */
    @Test
    void listUnsynchronizedNoExistingConnectors() {
        // init Ns4Kafka namespace
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // init connectorAsyncExecutor
        ConnectorAsyncExecutor connectorAsyncExecutor = Mockito.mock(ConnectorAsyncExecutor.class);
        Mockito.when(applicationContext.getBean(ConnectorAsyncExecutor.class,
                Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(connectorAsyncExecutor);

        // list of existing broker connectors
        Connector c1 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect1").build())
                .build();
        Connector c2 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect2").build())
                .build();
        Connector c3 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns1-connect1").build())
                .build();
        Connector c4 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns2-connect1").build())
                .build();
        Mockito.when(connectorAsyncExecutor.collectBrokerConnectors("local-name")).thenReturn(Single.just(List.of(
                c1, c2, c3, c4)));

        // list of existing Ns4Kafka access control entries
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect2"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns2-connect1"))
                .thenReturn(false);

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns1-connect1")
                                        .build())
                                .build()));

        // no connects exists into Ns4Kafka
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        connectorService.listUnsynchronizedConnectors(ns)
                .test()
                .assertValue(response -> response.size() == 3)
                .assertValue(response -> response.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")))
                .assertValue(response -> response.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")))
                .assertValue(response -> response.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns1-connect1")))
                .assertValue(response -> response.stream().noneMatch(connector -> connector.getMetadata().getName().equals("ns2-connect1")));
    }

    /**
     * Test the listing of unsynchronized connectors when they are all synchronized
     */
    @Test
    void listUnsynchronizedAllExistingConnectors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // init connectorAsyncExecutor
        ConnectorAsyncExecutor connectorAsyncExecutor = Mockito.mock(ConnectorAsyncExecutor.class);
        Mockito.when(applicationContext.getBean(ConnectorAsyncExecutor.class,
                Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(connectorAsyncExecutor);

        // list of existing broker connectors
        Connector c1 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect1").build())
                .build();
        Connector c2 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect2").build())
                .build();
        Connector c3 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns1-connect1").build())
                .build();
        Connector c4 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns2-connect1").build())
                .build();
        Mockito.when(connectorAsyncExecutor.collectBrokerConnectors("local-name")).thenReturn(Single.just(List.of(
                c1, c2, c3, c4)));

        // list of existing broker connects
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1, c2, c3, c4));

        // list of existing Ns4Kafka access control entries
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect2"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns2-connect1"))
                .thenReturn(false);

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns1-connect1")
                                        .build())
                                .build()
                ));

        // all connects exists into ns4kfk
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1, c2, c3, c4));

        connectorService.listUnsynchronizedConnectors(ns)
                .test()
                .assertValue(response -> response.size() == 0);
    }

    /**
     * Test the listing of unsynchronized connectors when some are synchronized and some not
     */
    @Test
    void listUnsynchronizedPartialExistingConnectors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // init connectorAsyncExecutor
        ConnectorAsyncExecutor connectorAsyncExecutor = Mockito.mock(ConnectorAsyncExecutor.class);
        Mockito.when(applicationContext.getBean(ConnectorAsyncExecutor.class,
                Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(connectorAsyncExecutor);
        
        // list of existing broker connectors
        Connector c1 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect1").build())
                .build();
        Connector c2 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect2").build())
                .build();
        Connector c3 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns1-connect1").build())
                .build();
        Connector c4 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns2-connect1").build())
                .build();
        
        Mockito.when(connectorAsyncExecutor.collectBrokerConnectors("local-name")).thenReturn(Single.just(List.of(
                c1, c2, c3, c4)));
        
        // list of existing broker connects
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1, c2, c3, c4));


        // list of existing Ns4Kafka access control entries
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns-connect2"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns1-connect1"))
                .thenReturn(true);
        Mockito.when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.ResourceType.CONNECT, "ns2-connect1"))
                .thenReturn(false);

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns-")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns1-connect1")
                                        .build())
                                .build()
                ));

        // partial number of topics exists into ns4kfk
        // all connects exists into ns4kfk
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1));

        connectorService.listUnsynchronizedConnectors(ns)
                .test()
                .assertValue(response -> response.size() == 2)
                .assertValue(response -> response.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")))
                .assertValue(response -> response.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns1-connect1")))
                .assertValue(response -> response.stream().noneMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")))
                .assertValue(response -> response.stream().noneMatch(connector -> connector.getMetadata().getName().equals("ns2-connect1")));
    }

    /**
     * Tests to delete a connector
     */
    @Test
    void deleteConnectorSuccess() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect1").build())
                .spec(Connector.ConnectorSpec.builder().connectCluster("local-name").build())
                .build();

        when(connectorClient.delete(ConnectorClientProxy.PROXY_SECRET, ns.getMetadata().getCluster(),
                "local-name", "ns-connect1")).thenReturn(Maybe.just(HttpResponse.ok()));

        doNothing().when(connectorRepository).delete(connector);

        connectorService.delete(ns, connector)
                .test()
                .assertValue(response -> response.getStatus().equals(HttpStatus.OK));

        verify(connectorClient, times(1)).delete(ConnectorClientProxy.PROXY_SECRET, ns.getMetadata().getCluster(),
                "local-name", "ns-connect1");

        verify(connectorRepository, times(1)).delete(connector);
    }

    /**
     * Tests to delete a connector when the cluster is not responding
     */
    @Test
    void deleteConnectorConnectClusterError() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns-connect1").build())
                .spec(Connector.ConnectorSpec.builder().connectCluster("local-name").build())
                .build();

        when(connectorClient.delete(ConnectorClientProxy.PROXY_SECRET, ns.getMetadata().getCluster(),
                "local-name", "ns-connect1")).thenReturn(Maybe.error(new HttpClientResponseException("Error", HttpResponse.serverError())));

        connectorService.delete(ns, connector)
                .test()
                .assertError(HttpClientResponseException.class);

        verify(connectorRepository, never()).delete(connector);
    }
}
