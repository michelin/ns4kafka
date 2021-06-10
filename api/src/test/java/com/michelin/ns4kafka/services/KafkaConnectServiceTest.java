package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.*;
import com.michelin.ns4kafka.services.executors.ConnectorAsyncExecutor;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.ResourceValidator;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@ExtendWith(MockitoExtension.class)
public class KafkaConnectServiceTest {

    @Mock
    AccessControlEntryService accessControlEntryService;
    @Mock
    KafkaConnectClient kafkaConnectClient;
    @Mock
    ConnectorRepository connectorRepository;
    @Mock
    ApplicationContext applicationContext;

    @InjectMocks
    KafkaConnectService kafkaConnectService;

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

        List<Connector> actual = kafkaConnectService.findAllForNamespace(ns);

        Assertions.assertTrue(actual.isEmpty());
    }

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
        Connector c6 = Connector.builder()
                .metadata(ObjectMeta.builder().name("ns3-connect1").build())
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

        List<Connector> actual = kafkaConnectService.findAllForNamespace(ns);

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

        Optional<Connector> actual = kafkaConnectService.findByName(ns, "ns-connect1");

        Assertions.assertTrue(actual.isEmpty());
    }

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

        Optional<Connector> actual = kafkaConnectService.findByName(ns, "ns-connect1");

        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("ns-connect1", actual.get().getMetadata().getName());
    }

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

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals("Invalid value wrong for spec.connectCluster: Value must be one of [local-name]", actual.get(0));
    }

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

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals("Invalid value for spec.config.'connector.class': Value must be non-null", actual.get(0));
    }

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
        Mockito.when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(List.of());

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals("Failed to find any class that implements Connector and which name matches org.apache.kafka.connect.file.FileStreamSinkConnector", actual.get(0));

    }

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
        Mockito.when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(List.of(new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1")));

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals("Invalid value null for configuration missing.field: Value must be non-null", actual.get(0));

    }

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
        Mockito.when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(List.of(new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1")));


        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void validateRemotely_Errors() {
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
        Mockito.when(kafkaConnectClient.validate(ArgumentMatchers.eq("local"), ArgumentMatchers.eq("local-name"), ArgumentMatchers.any(), ArgumentMatchers.anyMap()))
                .thenReturn(configInfos);

        List<String> actual = kafkaConnectService.validateRemotely(ns, connector);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertIterableEquals(List.of("error_message"), actual);
    }

    @Test
    void validateRemotely_Success() {
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
        Mockito.when(kafkaConnectClient.validate(ArgumentMatchers.eq("local"), ArgumentMatchers.eq("local-name"), ArgumentMatchers.any(), ArgumentMatchers.anyMap()))
                .thenReturn(configInfos);

        List<String> actual = kafkaConnectService.validateRemotely(ns, connector);

        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void listUnsynchronizedNoExistingConnectors() throws InterruptedException, ExecutionException, TimeoutException {

        // init ns4kfk namespace
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
        Mockito.when(connectorAsyncExecutor.collectBrokerConnectors("local-name")).thenReturn(List.of(
                c1, c2, c3, c4));

        // list of existing ns4kfk access control entries
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

        // no connects exists into ns4kfk
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of());
        List<Connector> actual = kafkaConnectService.listUnsynchronizedConnectors(ns);

        Assertions.assertEquals(3, actual.size());
        // contains
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns1-connect1")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns2-connect1")));

    }

    @Test
    void listUnsynchronizedAllExistingConnectors() throws InterruptedException, ExecutionException, TimeoutException {

        // init ns4kfk namespace
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
        Mockito.when(connectorAsyncExecutor.collectBrokerConnectors("local-name")).thenReturn(List.of(
                c1, c2, c3, c4));

        // list of existing broker connects
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1, c2, c3, c4));

        // list of existing ns4kfk access control entries
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

        List<Connector> actual = kafkaConnectService.listUnsynchronizedConnectors(ns);

        Assertions.assertEquals(0, actual.size());

    }

    @Test
    void listUnsynchronizedPartialExistingConnectors() throws InterruptedException, ExecutionException, TimeoutException {

        // init ns4kfk namespace
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
        
        Mockito.when(connectorAsyncExecutor.collectBrokerConnectors("local-name")).thenReturn(List.of(
                c1, c2, c3, c4));
        
        // list of existing broker connects
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1, c2, c3, c4));


        // list of existing ns4kfk access control entries
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

        List<Connector> actual = kafkaConnectService.listUnsynchronizedConnectors(ns);

        Assertions.assertEquals(2, actual.size());
        // contains
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns1-connect1")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns2-connect1")));


    }

}
