package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.ResourceValidator;
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
public class KafkaConnectServiceTest {

    @Mock
    AccessControlEntryService accessControlEntryService;
    @Mock
    KafkaConnectClient kafkaConnectClient;
    @Mock
    ConnectorRepository connectorRepository;

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
        Mockito.when(connectorRepository.findAllForCluster("local"))
                .thenReturn(List.of(c1, c2, c3, c4));

        List<Connector> actual = kafkaConnectService.findAllForNamespace(ns);

        Assertions.assertEquals(3, actual.size());
        // contains
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("other-connect1")));
        // doesn't contain
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("other-connect2")));
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
    void validateLocallyNoClassName() {
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Connector.ConnectorSpec.builder()
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
    void validateLocallyFailure() {
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


        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertFalse(actual.stream().anyMatch(s -> s.startsWith("Failed to find any class that implements Connector and which name matches")));
        Assertions.assertTrue(actual.stream().anyMatch(s -> s.startsWith("Invalid value null for configuration missing.field")));
        Assertions.assertTrue(actual.size() > 0);
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

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void validateRemotelyMissingClassName() {
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
        Mockito.when(kafkaConnectClient.connectPlugins("local", "local-name"))
                .thenReturn(List.of());

        List<String> actual = kafkaConnectService.validateRemotely(ns, connector);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertIterableEquals(List.of("Failed to find any class that implements Connector and which name matches com.michelin.NoClass"), actual);
    }
}
