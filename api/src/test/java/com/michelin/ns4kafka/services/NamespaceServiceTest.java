package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig.ConnectConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class NamespaceServiceTest {

    @Mock
    NamespaceRepository namespaceRepository;
    @Mock
    TopicService topicService;
    @Mock
    RoleBindingService roleBindingService;
    @Mock
    AccessControlEntryService accessControlEntryService;
    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;

    @InjectMocks
    NamespaceService namespaceService;

    @Test
    void validationCreationNoClusterFail() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        List<String> result = namespaceService.validateCreation(ns);
        assertEquals(1, result.size());
        assertEquals("Invalid value local for cluster: Cluster doesn't exist", result.get(0));

    }

    @Test
    void validationCreationKafkaUserAlreadyExistFail() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user")
                        .build())
                .build();

        Namespace ns2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace2")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user")
                        .build())
                .build();
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig1 = new KafkaAsyncExecutorConfig("local");

        when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1));
        when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of(ns2));

        List<String> result = namespaceService.validateCreation(ns);
        assertEquals(1, result.size());
        assertEquals("Invalid value user for user: KafkaUser already exists", result.get(0));

    }

    @Test
    void validateCreationNoNamespaceSuccess() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user")
                        .build())
                .build();

        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig1 = new KafkaAsyncExecutorConfig("local");

        when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1));
        when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        List<String> result = namespaceService.validateCreation(ns);
        assertTrue(result.isEmpty());
    }

    @Test
    void validateCreationANamespaceAlreadyExistsSuccess() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user")
                        .build())
                .build();
        Namespace ns2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace2")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user2")
                        .build())
                .build();

        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig1 = new KafkaAsyncExecutorConfig("local");

        when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1));
        when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of(ns2));

        List<String> result = namespaceService.validateCreation(ns);
        assertTrue(result.isEmpty());
    }

    @Test
    void validateSuccess() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user")
                        .build())
                .build();

        KafkaAsyncExecutorConfig kafka = new KafkaAsyncExecutorConfig("local");
        kafka.setConnects(Map.of("local-name", new ConnectConfig()));

        when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafka));

        List<String> result = namespaceService.validate(ns);

        assertTrue(result.isEmpty());

    }

    @Test
    void validateFail() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user")
                        .build())
                .build();

        Namespace ns2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace2")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user2")
                        .build())
                .build();

        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig1 = new KafkaAsyncExecutorConfig("local");
        kafkaAsyncExecutorConfig1.setConnects(Map.of("other-connect-config", new ConnectConfig()));

        when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1));

        List<String> result = namespaceService.validate(ns);

        assertEquals(1, result.size());
        assertEquals("Invalid value local-name for Connect Cluster: Connect Cluster doesn't exist", result.get(0));
    }

    @Test
    void listAll() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user")
                        .build())
                .build();
        Namespace ns2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace2")
                        .cluster("local")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user2")
                        .build())
                .build();
        Namespace ns3 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace3")
                        .cluster("other-cluster")
                        .build())
                .spec(NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .kafkaUser("user3")
                        .build())
                .build();

        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig1 = new KafkaAsyncExecutorConfig("local");
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig2 = new KafkaAsyncExecutorConfig("other-cluster");

        when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1, kafkaAsyncExecutorConfig2));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(ns, ns2));
        when(namespaceRepository.findAllForCluster("other-cluster")).thenReturn(List.of(ns3));

        List<Namespace> result = namespaceService.listAll();

        assertEquals(3, result.size());
        assertTrue(result.containsAll(List.of(ns, ns3, ns2)));
    }

    @Test
    void isNamespaceEmptySuccess() {
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
        when(topicService.findAllForNamespace(ns)).thenReturn(List.of());
        when(roleBindingService.list("namespace")).thenReturn(List.of());
        when(accessControlEntryService.findAllNamespaceIsGrantor(ns)).thenReturn(List.of());

        var result = namespaceService.isNamespaceEmpty(ns);
        assertTrue(result);

    }
}
