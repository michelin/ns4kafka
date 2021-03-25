package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
public class ConnectControllerTest {

    @Mock
    KafkaConnectService kafkaConnectService;

    @Mock
    NamespaceRepository namespaceRepository;

    @InjectMocks
    ConnectController connectController;

    @Test
    void listEmptyConnectors() {
        Mockito.when(kafkaConnectService.findByNamespace("test"))
                .thenReturn(List.of());

        List<Connector> actual = connectController.list("test");
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void listMultipleConnectors() {
        Mockito.when(kafkaConnectService.findByNamespace("test"))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()
                ));

        List<Connector> actual = connectController.list("test");
        Assertions.assertEquals(2, actual.size());
    }

    @Test
    void getConnectorEmpty() {
        Mockito.when(kafkaConnectService.findByName("test", "missing"))
                .thenReturn(Optional.empty());

        Optional<Connector> actual = connectController.getConnector("test", "missing");
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void getConnector() {
        Mockito.when(kafkaConnectService.findByName("test", "connect1"))
                .thenReturn(Optional.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build()
                ));

        Optional<Connector> actual = connectController.getConnector("test", "connect1");
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("connect1", actual.get().getMetadata().getName());
    }

    @Test
    void deleteConnecortNotOwned() {
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect("test", "connect1"))
                .thenReturn(false);

        Assertions.assertThrows(ResourceValidationException.class, () -> connectController.deleteConnector("test", "connect1"));
    }

    @Test
    void deleteConnectorOwned() {
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect("test", "connect1"))
                .thenReturn(true);

        Assertions.assertDoesNotThrow(() -> connectController.deleteConnector("test", "connect1"));
    }

    @Test
    void createConnectorNotOwner() {
        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect("test", "connect1"))
                .thenReturn(false);

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class, () -> connectController.apply("test", connector));
        Assertions.assertLinesMatch(List.of("Invalid value connect1 for name: Namespace not OWNER of this connector"), actual.getValidationErrors());
    }

    @Test
    void createConnectorLocalErrors() {
        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect("test", "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.validateLocally("test", connector))
                .thenReturn(List.of("Local Validation Error 1"));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class, () -> connectController.apply("test", connector));
        Assertions.assertLinesMatch(List.of("Local Validation Error 1"), actual.getValidationErrors());
    }

    @Test
    void createConnectorRemoteErrors() {
        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect("test", "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.validateLocally("test", connector))
                .thenReturn(List.of());
        Mockito.when(kafkaConnectService.validateRemotely("test", connector))
                .thenReturn(List.of("Remote Validation Error 1"));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class, () -> connectController.apply("test", connector));
        Assertions.assertLinesMatch(List.of("Remote Validation Error 1"), actual.getValidationErrors());
    }

    @Test
    void createConnectorSuccess() {
        Connector connector = Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build();
        Connector expected = Connector.builder()
                .metadata(ObjectMeta.builder().name("connect1").build())
                .status(Connector.ConnectorStatus.builder().state(Connector.TaskState.UNASSIGNED).build())
                .build();

        Mockito.when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect("test", "connect1"))
                .thenReturn(true);
        Mockito.when(kafkaConnectService.validateLocally("test", connector))
                .thenReturn(List.of());
        Mockito.when(kafkaConnectService.validateRemotely("test", connector))
                .thenReturn(List.of());
        Mockito.when(kafkaConnectService.createOrUpdate("test", connector))
                .thenReturn(expected);

        Connector actual = connectController.apply("test", connector);

        Assertions.assertEquals(expected.getStatus().getState(), actual.getStatus().getState());

    }
}
