package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
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
    void findByNamespaceNone(){
        Mockito.when(namespaceRepository.findByName("namespace"))
                .thenReturn(Optional.of(Namespace.builder().cluster("local").build()));

        Mockito.when(accessControlEntryRepository.findAllGrantedToNamespace("namespace"))
                .thenReturn(List.of());
        Mockito.when(kafkaConnectClient.listAll("local"))
                .thenReturn(Map.of());

        List<Connector> actual = kafkaConnectService.findByNamespace("namespace");

        Assertions.assertTrue(actual.isEmpty());
    }
}
