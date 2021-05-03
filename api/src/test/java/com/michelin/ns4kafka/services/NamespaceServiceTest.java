package com.michelin.ns4kafka.services;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.repositories.NamespaceRepository;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NamespaceServiceTest {

    @Mock
    NamespaceRepository namespaceRepository;
    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;

    @InjectMocks
    NamespaceService namespaceService;

    @BeforeEach
    public void init() {
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig("test_cluster");
        //TODO Can't managed to stock a value in the list
        when(kafkaAsyncExecutorConfigList.stream()).thenReturn(Stream.of(kafkaAsyncExecutorConfig));

        lenient().when(namespaceRepository.findAllForCluster("test_cluster"))
            .thenReturn(List.of(Namespace.builder()
                .metadata(ObjectMeta.builder()
                    .cluster("test_cluster")
                    .build())
                .spec(NamespaceSpec.builder()
                    .kafkaUser("test_user")
                    .build())
                .build()));

    }


    @Test
    void validationCreationFailBadKafkaUser() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .cluster("test_cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("test_user")
                .build())
            .build();

        List<String> actual = namespaceService.validateCreation(ns);
        Assertions.assertFalse(actual.isEmpty());
    }

    @Test
    void validationCreationFailBadClusterName() {
        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .cluster("false")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("new_user")
                .build())
            .build();

        List<String> actual = namespaceService.validateCreation(ns);
        Assertions.assertFalse(actual.isEmpty());
    }

    @Test
    void validateCreationSuccess() {

        Namespace ns = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .cluster("test_cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("new_user")
                .build())
            .build();

        List<String> actual = namespaceService.validateCreation(ns);
        Assertions.assertTrue(actual.isEmpty());
    }
}
