package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig.ConnectConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
    ConnectorService connectorService;
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

        Mockito.when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        List<String> result = namespaceService.validateCreation(ns);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Invalid value local for cluster: Cluster doesn't exist", result.get(0));

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

        Mockito.when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1));
        Mockito.when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of(ns2));

        List<String> result = namespaceService.validateCreation(ns);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Invalid value user for user: KafkaUser already exists", result.get(0));

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

        Mockito.when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1));
        Mockito.when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of());

        List<String> result = namespaceService.validateCreation(ns);
        Assertions.assertTrue(result.isEmpty());
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

        Mockito.when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1));
        Mockito.when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of(ns2));

        List<String> result = namespaceService.validateCreation(ns);
        Assertions.assertTrue(result.isEmpty());
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

        Mockito.when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafka));

        List<String> result = namespaceService.validate(ns);

        Assertions.assertTrue(result.isEmpty());

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

        Mockito.when(kafkaAsyncExecutorConfigList.stream())
                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1));

        List<String> result = namespaceService.validate(ns);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Invalid value local-name for Connect Cluster: Connect Cluster doesn't exist", result.get(0));
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

        Mockito.when(kafkaAsyncExecutorConfigList.stream())

                .thenReturn(Stream.of(kafkaAsyncExecutorConfig1, kafkaAsyncExecutorConfig2));
        Mockito.when(namespaceRepository.findAllForCluster("local"))
                .thenReturn(List.of(ns, ns2));
        Mockito.when(namespaceRepository.findAllForCluster("other-cluster"))
                .thenReturn(List.of(ns3));

        List<Namespace> result = namespaceService.listAll();

        Assertions.assertEquals(3, result.size());
        Assertions.assertTrue(result.containsAll(List.of(ns, ns3, ns2)));
    }

    @Test
    void listAllNamespaceResourcesEmpty() {
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

        Mockito.when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(connectorService.findAllForNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(roleBindingService.list("namespace"))
                .thenReturn(List.of());
        Mockito.when(accessControlEntryService.findAllForNamespace(ns))
                .thenReturn(List.of());


        List<String> result = namespaceService.listAllNamespaceResources(ns);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void listAllNamespaceResourcesTopic() {
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

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
                      .name("topic")
                      .namespace("namespace")
                      .build())
            .build();

        Mockito.when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic));
        Mockito.when(connectorService.findAllForNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(roleBindingService.list("namespace"))
                .thenReturn(List.of());
        Mockito.when(accessControlEntryService.findAllForNamespace(ns))
                .thenReturn(List.of());


        List<String> result = namespaceService.listAllNamespaceResources(ns);
        Assertions.assertEquals(1,result.size());
        Assertions.assertEquals("Topic/topic",result.get(0));
    }

    @Test
    void listAllNamespaceResourcesConnect() {
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

        Connector connector = Connector.builder()
            .metadata(ObjectMeta.builder()
                      .name("connector")
                      .namespace("namespace")
                      .build())
            .build();

        Mockito.when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(connectorService.findAllForNamespace(ns))
                .thenReturn(List.of(connector));
        Mockito.when(roleBindingService.list("namespace"))
                .thenReturn(List.of());
        Mockito.when(accessControlEntryService.findAllForNamespace(ns))
                .thenReturn(List.of());


        List<String> result = namespaceService.listAllNamespaceResources(ns);
        Assertions.assertEquals(1,result.size());
        Assertions.assertEquals("Connector/connector",result.get(0));
    }

    @Test
    void listAllNamespaceResourcesRoleBinding() {
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

        RoleBinding rb = RoleBinding.builder()
            .metadata(ObjectMeta.builder()
                      .name("rolebinding")
                      .namespace("namespace")
                      .build())
            .build();

        Mockito.when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(connectorService.findAllForNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(roleBindingService.list("namespace"))
                .thenReturn(List.of(rb));
        Mockito.when(accessControlEntryService.findAllForNamespace(ns))
                .thenReturn(List.of());


        List<String> result = namespaceService.listAllNamespaceResources(ns);
        Assertions.assertEquals(1,result.size());
        Assertions.assertEquals("RoleBinding/rolebinding",result.get(0));
    }

    @Test
    void listAllNamespaceResourcesAccessControlEntry() {
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

        AccessControlEntry ace = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ace")
                      .namespace("namespace")
                      .build())
            .build();

        Mockito.when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(connectorService.findAllForNamespace(ns))
                .thenReturn(List.of());
        Mockito.when(roleBindingService.list("namespace"))
                .thenReturn(List.of());
        Mockito.when(accessControlEntryService.findAllForNamespace(ns))
                .thenReturn(List.of(ace));


        List<String> result = namespaceService.listAllNamespaceResources(ns);
        Assertions.assertEquals(1,result.size());
        Assertions.assertEquals("AccessControlEntry/ace",result.get(0));
    }

}
