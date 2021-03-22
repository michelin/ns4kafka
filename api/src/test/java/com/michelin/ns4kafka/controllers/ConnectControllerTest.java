package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import com.michelin.ns4kafka.validation.ResourceValidationException;
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

    @InjectMocks ConnectController connectController;

    @Test
    void listEmptyConnectors(){
        Mockito.when(kafkaConnectService.findByNamespace("test"))
                .thenReturn(List.of());

        List<Connector> actual = connectController.list("test");
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void listMultipleConnectors(){
        Mockito.when(kafkaConnectService.findByNamespace("test"))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()
                        ));

        List<Connector> actual = connectController.list("test");
        Assertions.assertEquals(2, actual.size());
    }

    @Test
    void getConnectorEmpty(){
        Mockito.when(kafkaConnectService.findByName("test", "missing"))
                .thenReturn(Optional.empty());

        Optional<Connector> actual = connectController.getConnector("test", "missing");
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void getConnector(){
        Mockito.when(kafkaConnectService.findByName("test","connect1"))
                .thenReturn(Optional.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build()
                ));

        Optional<Connector> actual = connectController.getConnector("test", "connect1");
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("connect1", actual.get().getMetadata().getName());
    }

    @Test
    void deleteConnecortNotOwned(){
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect("test","connect1"))
                .thenReturn(false);

        Assertions.assertThrows(ResourceValidationException.class, () -> connectController.deleteConnector("test", "connect1"));
    }

    @Test
    void deleteConnectorOwned(){
        Mockito.when(kafkaConnectService.isNamespaceOwnerOfConnect("test", "connect1"))
                .thenReturn(true);

        Assertions.assertDoesNotThrow(() -> connectController.deleteConnector("test", "connect1"));
    }

    @Test
    void createConnectorNotOwner(){ }

    @Test
    void createConnectorLocalErrors(){ }

    @Test
    void createConnectorRemoteErrors(){ }

    @Test
    void createConnectorSuccess(){ }
}
