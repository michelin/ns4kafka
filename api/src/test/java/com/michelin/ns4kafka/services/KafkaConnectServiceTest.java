package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorInfo;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorPluginInfo;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStatus;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorType;
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

    @InjectMocks
    KafkaConnectService kafkaConnectService;

    @Test
    void findByNamespaceNone() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(kafkaConnectClient.listAll("local"))
                .thenReturn(Map.of());

        List<Connector> actual = kafkaConnectService.list(ns);

        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void findByNamespaceMultiple() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        ConnectorStatus c1 = new ConnectorStatus();
        ConnectorStatus c2 = new ConnectorStatus();
        ConnectorStatus c3 = new ConnectorStatus();
        c1.setInfo(new ConnectorInfo("ns-connect1", Map.of(), List.of(), ConnectorType.SINK));
        c2.setInfo(new ConnectorInfo("ns-connect2", Map.of(), List.of(), ConnectorType.SINK));
        c3.setInfo(new ConnectorInfo("other-connect1", Map.of(), List.of(), ConnectorType.SINK));

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
        Mockito.when(kafkaConnectClient.listAll("local"))
                .thenReturn(Map.of(c1.getInfo().name(), c1,
                        c2.getInfo().name(), c2,
                        c3.getInfo().name(), c3)
                );

        List<Connector> actual = kafkaConnectService.list(ns);

        Assertions.assertEquals(2, actual.size());
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")));
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("other-connect1")));
    }

    @Test
    void findByNameNotFound() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(kafkaConnectClient.listAll("local"))
                .thenReturn(Map.of());

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
                .build();
        ConnectorStatus c1 = new ConnectorStatus();
        ConnectorStatus c2 = new ConnectorStatus();
        ConnectorStatus c3 = new ConnectorStatus();
        c1.setInfo(new ConnectorInfo("ns-connect1", Map.of(), List.of(), ConnectorType.SINK));
        c2.setInfo(new ConnectorInfo("ns-connect2", Map.of(), List.of(), ConnectorType.SINK));
        c3.setInfo(new ConnectorInfo("other-connect1", Map.of(), List.of(), ConnectorType.SINK));

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
        Mockito.when(kafkaConnectClient.listAll("local"))
                .thenReturn(Map.of(c1.getInfo().name(), c1,
                        c2.getInfo().name(), c2,
                        c3.getInfo().name(), c3)
                );

        Optional<Connector> actual = kafkaConnectService.findByName(ns, "ns-connect1");

        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("ns-connect1", actual.get().getMetadata().getName());
    }

    @Test
    void validateLocallyNoClassName(){
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Map.of())
                .build();
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertEquals(1, actual.size());
    }
    @Test
    void validateLocallyMissingClassName(){
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Map.of("connector.class","com.michelin.NoClass"))
                .build();
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        Mockito.when(kafkaConnectClient.connectPlugins("local"))
                .thenReturn(List.of());

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertIterableEquals(List.of("Failed to find any class that implements Connector and which name matches com.michelin.NoClass"),actual);
    }
    @Test
    void validateLocallyFailure(){
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Map.of("connector.class","org.apache.kafka.connect.file.FileStreamSinkConnector"))
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
                        .build())
                .build();

        Mockito.when(kafkaConnectClient.connectPlugins("local"))
                .thenReturn(List.of(new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1")));

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertFalse(actual.stream().anyMatch(s -> s.startsWith("Failed to find any class that implements Connector and which name matches")));
        Assertions.assertTrue(actual.stream().anyMatch(s -> s.startsWith("Invalid value null for configuration missing.field")));
        Assertions.assertTrue(actual.size() > 0);
    }

    @Test
    void validateLocallySuccess(){
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Map.of("connector.class","org.apache.kafka.connect.file.FileStreamSinkConnector"))
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
                        .build())
                .build();
        Mockito.when(kafkaConnectClient.connectPlugins("local"))
                .thenReturn(List.of(new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1")));

        List<String> actual = kafkaConnectService.validateLocally(ns, connector);
        Assertions.assertTrue(actual.isEmpty());
    }
}
