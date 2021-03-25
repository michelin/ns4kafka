package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorInfo;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorPluginInfo;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStatus;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorType;
import com.michelin.ns4kafka.validation.ConnectValidator;
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
    NamespaceRepository namespaceRepository;
    @Mock
    AccessControlEntryRepository accessControlEntryRepository;
    @Mock
    KafkaConnectClient kafkaConnectClient;

    @InjectMocks
    KafkaConnectService kafkaConnectService;

    @Test
    void findByNamespaceNone() {
        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(Namespace.builder().cluster("local").build()));

        Mockito.when(accessControlEntryRepository.findAllGrantedToNamespace("namespace"))
                .thenReturn(List.of());
        Mockito.when(kafkaConnectClient.listAll("local"))
                .thenReturn(Map.of());

        List<Connector> actual = kafkaConnectService.findByNamespace("namespace");

        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void findByNamespaceMultiple() {
        ConnectorStatus c1 = new ConnectorStatus();
        ConnectorStatus c2 = new ConnectorStatus();
        ConnectorStatus c3 = new ConnectorStatus();
        c1.setInfo(new ConnectorInfo("ns-connect1", Map.of(), List.of(), ConnectorType.SINK));
        c2.setInfo(new ConnectorInfo("ns-connect2", Map.of(), List.of(), ConnectorType.SINK));
        c3.setInfo(new ConnectorInfo("other-connect1", Map.of(), List.of(), ConnectorType.SINK));

        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(Namespace.builder().cluster("local").build()));

        Mockito.when(accessControlEntryRepository.findAllGrantedToNamespace("namespace"))
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

        List<Connector> actual = kafkaConnectService.findByNamespace("namespace");

        Assertions.assertEquals(2, actual.size());
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect1")));
        Assertions.assertTrue(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("ns-connect2")));
        Assertions.assertFalse(actual.stream().anyMatch(connector -> connector.getMetadata().getName().equals("other-connect1")));
    }

    @Test
    void findByNameNotFound() {
        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(Namespace.builder().cluster("local").build()));

        Mockito.when(accessControlEntryRepository.findAllGrantedToNamespace("namespace"))
                .thenReturn(List.of());
        Mockito.when(kafkaConnectClient.listAll("local"))
                .thenReturn(Map.of());

        Optional<Connector> actual = kafkaConnectService.findByName("namespace", "ns-connect1");

        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void findByNameFound() {
        ConnectorStatus c1 = new ConnectorStatus();
        ConnectorStatus c2 = new ConnectorStatus();
        ConnectorStatus c3 = new ConnectorStatus();
        c1.setInfo(new ConnectorInfo("ns-connect1", Map.of(), List.of(), ConnectorType.SINK));
        c2.setInfo(new ConnectorInfo("ns-connect2", Map.of(), List.of(), ConnectorType.SINK));
        c3.setInfo(new ConnectorInfo("other-connect1", Map.of(), List.of(), ConnectorType.SINK));

        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(Namespace.builder().cluster("local").build()));

        Mockito.when(accessControlEntryRepository.findAllGrantedToNamespace("namespace"))
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

        Optional<Connector> actual = kafkaConnectService.findByName("namespace", "ns-connect1");

        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("ns-connect1", actual.get().getMetadata().getName());
    }

    @Test
    void validateLocallyNoClassName(){
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Map.of())
                .build();
        Namespace namespace = Namespace.builder()
                .cluster("local")
                .name("namespace")
                .build();
        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(namespace));

        List<String> actual = kafkaConnectService.validateLocally("namespace", connector);
        Assertions.assertEquals(1, actual.size());
    }
    @Test
    void validateLocallyMissingClassName(){
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Map.of("connector.class","com.michelin.NoClass"))
                .build();
        Namespace namespace = Namespace.builder()
                .cluster("local")
                .name("namespace")
                .build();
        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(namespace));
        Mockito.when(kafkaConnectClient.connectPlugins("local"))
                .thenReturn(List.of());

        List<String> actual = kafkaConnectService.validateLocally("namespace", connector);
        Assertions.assertEquals(1, actual.size());
        Assertions.assertIterableEquals(List.of("Failed to find any class that implements Connector and which name matches com.michelin.NoClass"),actual);
    }
    @Test
    void validateLocallyFailure(){
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Map.of("connector.class","org.apache.kafka.connect.file.FileStreamSinkConnector"))
                .build();
        Namespace namespace = Namespace.builder()
                .cluster("local")
                .name("namespace")
                .connectValidator(ConnectValidator.makeDefault())
                .build();
        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(namespace));
        Mockito.when(kafkaConnectClient.connectPlugins("local"))
                .thenReturn(List.of(new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1")));

        List<String> actual = kafkaConnectService.validateLocally("namespace", connector);
        Assertions.assertFalse(actual.stream().anyMatch(s -> s.startsWith("Failed to find any class that implements Connector and which name matches")));
        Assertions.assertTrue(actual.size() > 0);
    }

    @Test
    void validateLocallySuccess(){
        Connector connector = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .spec(Map.of("connector.class","org.apache.kafka.connect.file.FileStreamSinkConnector"))
                .build();
        Namespace namespace = Namespace.builder()
                .cluster("local")
                .name("namespace")
                .connectValidator(ConnectValidator.builder()
                        .classValidationConstraints(Map.of())
                        .sinkValidationConstraints(Map.of())
                        .sourceValidationConstraints(Map.of())
                        .validationConstraints(Map.of())
                        .build())
                .build();
        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(namespace));
        Mockito.when(kafkaConnectClient.connectPlugins("local"))
                .thenReturn(List.of(new ConnectorPluginInfo("org.apache.kafka.connect.file.FileStreamSinkConnector", ConnectorType.SINK, "v1")));

        List<String> actual = kafkaConnectService.validateLocally("namespace", connector);
        Assertions.assertTrue(actual.isEmpty());
    }
}
