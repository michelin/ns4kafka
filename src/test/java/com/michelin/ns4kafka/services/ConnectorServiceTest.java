package com.michelin.ns4kafka.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.clients.connect.KafkaConnectClient;
import com.michelin.ns4kafka.services.clients.connect.entities.ConfigInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConfigInfos;
import com.michelin.ns4kafka.services.clients.connect.entities.ConfigKeyInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConfigValueInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorPluginInfo;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorType;
import com.michelin.ns4kafka.services.executors.ConnectorAsyncExecutor;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.ResourceValidator;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.inject.qualifiers.Qualifiers;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

@ExtendWith(MockitoExtension.class)
class ConnectorServiceTest {
    @Mock
    AccessControlEntryService accessControlEntryService;

    @Mock
    KafkaConnectClient kafkaConnectClient;

    @Mock
    ConnectorRepository connectorRepository;

    @Mock
    ApplicationContext applicationContext;

    @InjectMocks
    ConnectorService connectorService;

    @Mock
    ConnectClusterService connectClusterService;

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

        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of());

        List<Connector> actual = connectorService.findAllForNamespace(ns);

        assertTrue(actual.isEmpty());
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

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns-")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("other-connect1")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.TOPIC)
                        .resource("ns-")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns2-")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.WRITE)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns3-")
                        .build())
                    .build()
            ));

        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of(c1, c2, c3, c4, c5));

        List<Connector> actual = connectorService.findAllForNamespace(ns);

        assertEquals(3, actual.size());
        // contains
        assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
        assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")));
        assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("other-connect1")));
        // doesn't contain
        Assertions.assertFalse(
            actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("other-connect2")));
        Assertions.assertFalse(
            actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns2-connect1")));
        Assertions.assertFalse(
            actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns3-connect1")));
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

        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of());

        Optional<Connector> actual = connectorService.findByName(ns, "ns-connect1");

        assertTrue(actual.isEmpty());
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

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns-")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("other-connect1")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.TOPIC)
                        .resource("ns-")
                        .build())
                    .build()
            ));

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of(AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .permission(AccessControlEntry.Permission.OWNER)
                    .grantedTo("namespace")
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resourceType(AccessControlEntry.AclType.CONNECT)
                    .resource("ns-")
                    .build())
                .build()));

        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of(c1, c2, c3));

        Optional<Connector> actual = connectorService.findByName(ns, "ns-connect1");

        assertTrue(actual.isPresent());
        assertEquals("ns-connect1", actual.get().getMetadata().getName());
    }

    @Test
    void findAllByConnectCluster() {
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
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("connect-cluster")
                .build())
            .build();

        Connector c2 = Connector.builder()
            .metadata(ObjectMeta.builder().name("ns-connect2").build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("connect-cluster2")
                .build())
            .build();

        Connector c3 = Connector.builder()
            .metadata(ObjectMeta.builder().name("other-connect1").build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("connect-cluster3")
                .build())
            .build();

        Connector c4 = Connector.builder()
            .metadata(ObjectMeta.builder().name("other-connect2").build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("connect-cluster4")
                .build())
            .build();

        Connector c5 = Connector.builder()
            .metadata(ObjectMeta.builder().name("ns2-connect1").build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("connect-cluster5")
                .build())
            .build();

        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of(c1, c2, c3, c4, c5));

        List<Connector> actual = connectorService.findAllByConnectCluster(ns, "connect-cluster");

        assertEquals(1, actual.size());
        assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
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

        when(connectClusterService.findAllByNamespaceWrite(ns)).thenReturn(List.of());
        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> {
                assertEquals(1, response.size());
                assertEquals(
                    "Invalid value \"wrong\" for field \"connectCluster\": value must be one of \"local-name\".",
                    response.get(0));
            })
            .verifyComplete();
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

        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> {
                assertEquals(1, response.size());
                assertEquals("Invalid empty value for field \"connector.class\": value must not be null.",
                    response.get(0));
            })
            .verifyComplete();
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

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
            .thenReturn(Mono.just(List.of()));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> {
                assertEquals(1, response.size());
                assertEquals(
                    "Invalid value \"org.apache.kafka.connect.file.FileStreamSinkConnector\" "
                        + "for field \"connector.class\": failed to find any class that implements connector and "
                        + "which name matches org.apache.kafka.connect.file.FileStreamSinkConnector.",
                    response.get(0));
            })
            .verifyComplete();
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

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
            .thenReturn(Mono.just(List.of(
                new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK,
                    "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> {
                assertEquals(1, response.size());
                assertEquals("Invalid empty value for field \"missing.field\": value must not be null.",
                    response.get(0));
            })
            .verifyComplete();
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

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
            .thenReturn(Mono.just(List.of(
                new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK,
                    "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> assertTrue(response.isEmpty()))
            .verifyComplete();
    }

    @Test
    void validateLocallySuccessWithNoConstraint() {
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

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
            .thenReturn(Mono.just(List.of(
                new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK,
                    "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> assertTrue(response.isEmpty()))
            .verifyComplete();
    }

    @Test
    void validateLocallySuccessWithNoValidationConstraint() {
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
                    .build())
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
            .thenReturn(Mono.just(List.of(
                new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK,
                    "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> assertTrue(response.isEmpty()))
            .verifyComplete();
    }

    @Test
    void validateLocallySuccessNoSinkValidationConstraint() {
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
                    .sourceValidationConstraints(Map.of())
                    .validationConstraints(Map.of())
                    .build())
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        when(kafkaConnectClient.connectPlugins("local", "local-name"))
            .thenReturn(Mono.just(List.of(
                new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK,
                    "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> assertTrue(response.isEmpty()))
            .verifyComplete();
    }

    @Test
    void validateLocallySuccessWithSelfDeployedConnectCluster() {
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
                .connectClusters(List.of())
                .build())
            .build();

        when(connectClusterService.findAllByNamespaceWrite(ns)).thenReturn(List.of(ConnectCluster.builder()
            .metadata(ObjectMeta.builder()
                .name("local-name")
                .build())
            .build()));
        when(kafkaConnectClient.connectPlugins("local", "local-name"))
            .thenReturn(Mono.just(List.of(
                new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK,
                    "v1"))));

        StepVerifier.create(connectorService.validateLocally(ns, connector))
            .consumeNextWith(response -> assertTrue(response.isEmpty()))
            .verifyComplete();
    }

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

        when(kafkaConnectClient.validate(
            ArgumentMatchers.eq("local"),
            ArgumentMatchers.eq("local-name"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()))
            .thenReturn(Mono.just(configInfos));

        StepVerifier.create(connectorService.validateRemotely(ns, connector))
            .consumeNextWith(response -> {
                assertEquals(1, response.size());
                assertEquals("Invalid \"connect1\": error_message.", response.get(0));
            })
            .verifyComplete();
    }

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

        when(kafkaConnectClient.validate(
            ArgumentMatchers.eq("local"),
            ArgumentMatchers.eq("local-name"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()))
            .thenReturn(Mono.just(configInfos));

        StepVerifier.create(connectorService.validateRemotely(ns, connector))
            .consumeNextWith(response -> assertTrue(response.isEmpty()))
            .verifyComplete();
    }

    @Test
    void listUnsynchronizedNoExistingConnectors() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ConnectorAsyncExecutor connectorAsyncExecutor = mock(ConnectorAsyncExecutor.class);
        when(applicationContext.getBean(ConnectorAsyncExecutor.class,
            Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(connectorAsyncExecutor);

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(ObjectMeta.builder().name("ns-connect-cluster").build())
            .build();

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
        Connector c5 = Connector.builder()
            .metadata(ObjectMeta.builder().name("ns1-connect2").build())
            .build();

        when(connectClusterService.findAllByNamespaceWrite(ns))
            .thenReturn(List.of(connectCluster));
        when(connectorAsyncExecutor.collectBrokerConnectors("local-name"))
            .thenReturn(Flux.fromIterable(List.of(c1, c2, c3, c4)));
        when(connectorAsyncExecutor.collectBrokerConnectors("ns-connect-cluster"))
            .thenReturn(Flux.fromIterable(List.of(c5)));

        // list of existing Ns4Kafka access control entries
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns-connect1"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns-connect2"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns1-connect1"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns1-connect2"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns2-connect1"))
            .thenReturn(false);

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns-")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns1-connect1")
                        .build())
                    .build()));

        // no connects exists into Ns4Kafka
        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of());

        StepVerifier.create(connectorService.listUnsynchronizedConnectors(ns))
            .consumeNextWith(connector -> assertEquals("ns-connect1", connector.getMetadata().getName()))
            .consumeNextWith(connector -> assertEquals("ns-connect2", connector.getMetadata().getName()))
            .consumeNextWith(connector -> assertEquals("ns1-connect1", connector.getMetadata().getName()))
            .consumeNextWith(connector -> assertEquals("ns1-connect2", connector.getMetadata().getName()))
            .verifyComplete();
    }

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

        ConnectorAsyncExecutor connectorAsyncExecutor = mock(ConnectorAsyncExecutor.class);
        when(applicationContext.getBean(ConnectorAsyncExecutor.class,
            Qualifiers.byName(ns.getMetadata().getCluster()))).thenReturn(connectorAsyncExecutor);


        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(ObjectMeta.builder().name("ns-connect-cluster").build())
            .build();

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
        Connector c5 = Connector.builder()
            .metadata(ObjectMeta.builder().name("ns1-connect2").build())
            .build();

        when(connectClusterService.findAllByNamespaceWrite(ns))
            .thenReturn(List.of(connectCluster));
        when(connectorAsyncExecutor.collectBrokerConnectors("local-name"))
            .thenReturn(Flux.fromIterable(List.of(c1, c2, c3, c4)));
        when(connectorAsyncExecutor.collectBrokerConnectors("ns-connect-cluster"))
            .thenReturn(Flux.fromIterable(List.of(c5)));
        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of(c1, c2, c3, c4, c5));

        // list of existing Ns4Kafka access control entries
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns-connect1"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns-connect2"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns1-connect1"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns1-connect2"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns2-connect1"))
            .thenReturn(false);

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns-")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns1-connect1")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns1-connect2")
                        .build())
                    .build()
            ));

        StepVerifier.create(connectorService.listUnsynchronizedConnectors(ns))
            .verifyComplete();
    }

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
        ConnectorAsyncExecutor connectorAsyncExecutor = mock(ConnectorAsyncExecutor.class);
        when(applicationContext.getBean(ConnectorAsyncExecutor.class,
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

        when(connectorAsyncExecutor.collectBrokerConnectors("local-name")).thenReturn(Flux.fromIterable(List.of(
            c1, c2, c3, c4)));

        // list of existing broker connects
        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of(c1, c2, c3, c4));


        // list of existing Ns4Kafka access control entries
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns-connect1"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns-connect2"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns1-connect1"))
            .thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource("namespace", AccessControlEntry.AclType.CONNECT,
            "ns2-connect1"))
            .thenReturn(false);

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns-")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.AclType.CONNECT)
                        .resource("ns1-connect1")
                        .build())
                    .build()
            ));

        when(connectorRepository.findAllForCluster("local"))
            .thenReturn(List.of(c1));

        StepVerifier.create(connectorService.listUnsynchronizedConnectors(ns))
            .consumeNextWith(connector -> assertEquals("ns-connect2", connector.getMetadata().getName()))
            .consumeNextWith(connector -> assertEquals("ns1-connect1", connector.getMetadata().getName()))
            .verifyComplete();
    }

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

        when(kafkaConnectClient.delete(ns.getMetadata().getCluster(),
            "local-name", "ns-connect1")).thenReturn(Mono.just(HttpResponse.ok()));

        doNothing().when(connectorRepository).delete(connector);

        StepVerifier.create(connectorService.delete(ns, connector))
            .consumeNextWith(response -> assertEquals(HttpStatus.OK, response.getStatus()))
            .verifyComplete();

        verify(kafkaConnectClient, times(1)).delete(ns.getMetadata().getCluster(),
            "local-name", "ns-connect1");

        verify(connectorRepository, times(1)).delete(connector);
    }

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

        when(kafkaConnectClient.delete(ns.getMetadata().getCluster(),
            "local-name", "ns-connect1")).thenReturn(
            Mono.error(new HttpClientResponseException("Error", HttpResponse.serverError())));

        StepVerifier.create(connectorService.delete(ns, connector))
            .consumeErrorWith(response -> assertEquals(HttpClientResponseException.class, response.getClass()))
            .verify();

        verify(connectorRepository, never()).delete(connector);
    }
}
