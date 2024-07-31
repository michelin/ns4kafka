package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.property.ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD;
import static com.michelin.ns4kafka.property.ManagedClusterProperties.KafkaProvider.SELF_MANAGED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.property.ManagedClusterProperties.ConnectProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NamespaceServiceTest {
    @Mock
    NamespaceRepository namespaceRepository;

    @Mock
    TopicService topicService;

    @Mock
    RoleBindingService roleBindingService;

    @Mock
    AclService aclService;

    @Mock
    ConnectorService connectorService;

    @Mock
    ConnectClusterService connectClusterService;

    @Mock
    ResourceQuotaService resourceQuotaService;

    @Mock
    List<ManagedClusterProperties> managedClusterProperties;

    @InjectMocks
    NamespaceService namespaceService;

    @Test
    void validationCreationNoClusterFail() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        List<String> result = namespaceService.validateCreation(ns);
        assertEquals(1, result.size());
        assertEquals("Invalid value \"local\" for field \"cluster\": cluster does not exist.", result.getFirst());

    }

    @Test
    void validateCreationNoNamespaceSuccess() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        ManagedClusterProperties managedClusterProperties1 = new ManagedClusterProperties("local");

        when(managedClusterProperties.stream())
            .thenReturn(Stream.of(managedClusterProperties1));

        List<String> result = namespaceService.validateCreation(ns);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldValidationSucceed() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        ManagedClusterProperties kafka = new ManagedClusterProperties("local");
        kafka.setConnects(Map.of("local-name", new ConnectProperties()));

        when(managedClusterProperties.stream())
            .thenReturn(Stream.of(kafka));

        List<String> result = namespaceService.validate(ns);

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldValidationSucceedWhenNoManagedCluster() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        when(managedClusterProperties.stream())
            .thenReturn(Stream.empty());

        List<String> result = namespaceService.validate(ns);

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldFailWhenConnectClusterDoesNotExist() {
        ManagedClusterProperties managedClusterProperties1 = new ManagedClusterProperties("local");
        managedClusterProperties1.setConnects(Map.of("other-connect-config", new ConnectProperties(),
            "other-connect-config2", new ConnectProperties()));

        when(managedClusterProperties.stream())
            .thenReturn(Stream.of(managedClusterProperties1));
        when(namespaceRepository.findAllForCluster("local"))
            .thenReturn(List.of());

        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        List<String> result = namespaceService.validate(ns);

        assertEquals(1, result.size());
        assertEquals("Invalid value \"local-name\" for field \"connectClusters\": connect cluster does not exist.",
            result.getFirst());
    }

    @Test
    void shouldFailWhenNotEditableConfigOnConfluentCloud() {
        ManagedClusterProperties managedClusterProperties1 = new ManagedClusterProperties("local", CONFLUENT_CLOUD);
        managedClusterProperties1.setConnects(Map.of("local-name", new ConnectProperties(),
            "local-name2", new ConnectProperties()));

        when(managedClusterProperties.stream())
            .thenReturn(Stream.of(managedClusterProperties1));
        when(namespaceRepository.findAllForCluster("local"))
            .thenReturn(List.of());

        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .topicValidator(TopicValidator.builder()
                    .validationConstraints(Map.of(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG,
                        ResourceValidator.Range.between(0.0, 1.0)))
                    .build())
                .build())
            .build();

        List<String> result = namespaceService.validate(ns);

        assertEquals(1, result.size());
        assertEquals("Invalid value \"min.cleanable.dirty.ratio\" for field \"validationConstraints\": "
                + "configuration not editable on a Confluent Cloud cluster.",
            result.getFirst());
    }

    @Test
    void shouldNotFailWhenNotEditableConfigOnSelfManaged() {
        ManagedClusterProperties managedClusterProperties1 = new ManagedClusterProperties("local", SELF_MANAGED);
        managedClusterProperties1.setConnects(Map.of("local-name", new ConnectProperties(),
            "local-name2", new ConnectProperties()));

        when(managedClusterProperties.stream())
            .thenReturn(Stream.of(managedClusterProperties1));
        when(namespaceRepository.findAllForCluster("local"))
            .thenReturn(List.of());

        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .topicValidator(TopicValidator.builder()
                    .validationConstraints(Map.of(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG,
                        ResourceValidator.Range.between(0.0, 1.0)))
                    .build())
                .build())
            .build();

        List<String> result = namespaceService.validate(ns);

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldFailWhenUserAlreadyExist() {
        Namespace ns2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace2")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        ManagedClusterProperties managedClusterProperties1 = new ManagedClusterProperties("local");
        managedClusterProperties1.setConnects(Map.of("local-name", new ConnectProperties()));

        when(managedClusterProperties.stream())
            .thenReturn(Stream.of(managedClusterProperties1));
        when(namespaceRepository.findAllForCluster("local"))
            .thenReturn(List.of(ns2));


        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        List<String> result = namespaceService.validate(ns);

        assertEquals(1, result.size());
        assertEquals("Invalid value \"user\" for field \"kafkaUser\": user already exists in another namespace.",
            result.getFirst());
    }

    @Test
    void shouldNotFailWhenUserAlreadyExistOnSameCluster() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        ManagedClusterProperties managedClusterProperties1 = new ManagedClusterProperties("local");
        managedClusterProperties1.setConnects(Map.of("local-name", new ConnectProperties()));

        when(managedClusterProperties.stream())
            .thenReturn(Stream.of(managedClusterProperties1));
        when(namespaceRepository.findAllForCluster("local"))
            .thenReturn(List.of(ns));

        List<String> result = namespaceService.validate(ns);

        assertEquals(0, result.size());
    }

    @Test
    void listAll() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();
        Namespace ns2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace2")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user2")
                .build())
            .build();
        Namespace ns3 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace3")
                .cluster("other-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user3")
                .build())
            .build();

        ManagedClusterProperties managedClusterProperties1 = new ManagedClusterProperties("local");
        ManagedClusterProperties managedClusterProperties2 = new ManagedClusterProperties("other-cluster");

        when(managedClusterProperties.stream())
            .thenReturn(Stream.of(managedClusterProperties1, managedClusterProperties2));
        when(namespaceRepository.findAllForCluster("local"))
            .thenReturn(List.of(ns, ns2));
        when(namespaceRepository.findAllForCluster("other-cluster"))
            .thenReturn(List.of(ns3));

        List<Namespace> result = namespaceService.listAll();

        assertEquals(3, result.size());
        assertTrue(result.containsAll(List.of(ns, ns3, ns2)));
    }

    @Test
    void listNamespaceWithNameParameter() {
        Namespace ns1 = Namespace.builder().metadata(Metadata.builder().name("ns1").build()).build();
        Namespace ns2 = Namespace.builder().metadata(Metadata.builder().name("ns2").build()).build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of(new ManagedClusterProperties("local")));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(ns1, ns2));

        assertEquals(List.of(ns1), namespaceService.findByWildcardName("ns1"));
    }

    @Test
    void listNoNamespaceWithNameParameter() {
        Namespace ns1 = Namespace.builder().metadata(Metadata.builder().name("ns1").build()).build();
        Namespace ns2 = Namespace.builder().metadata(Metadata.builder().name("ns2").build()).build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of(new ManagedClusterProperties("local")));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(ns1, ns2));

        assertTrue(namespaceService.findByWildcardName("ns4").isEmpty());
    }

    @Test
    void listAllNamespacesWithWildcardNameParameter() {
        Namespace ns1 = Namespace.builder().metadata(Metadata.builder().name("ns1").build()).build();
        Namespace ns2 = Namespace.builder().metadata(Metadata.builder().name("ns2").build()).build();
        Namespace ns3 = Namespace.builder().metadata(Metadata.builder().name("namespace1").build()).build();
        Namespace ns4 = Namespace.builder().metadata(Metadata.builder().name("ns3").build()).build();
        Namespace ns5 = Namespace.builder().metadata(Metadata.builder().name("namespace2").build()).build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of(new ManagedClusterProperties("local")));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(ns1, ns2, ns3, ns4, ns5));

        assertEquals(List.of(ns1, ns2, ns3, ns4, ns5), namespaceService.findByWildcardName("*"));
    }

    @Test
    void listNamespacesWithPrefixWildcardNameParameter() {
        Namespace ns1 = Namespace.builder().metadata(Metadata.builder().name("ns1").build()).build();
        Namespace ns2 = Namespace.builder().metadata(Metadata.builder().name("ns2").build()).build();
        Namespace ns3 = Namespace.builder().metadata(Metadata.builder().name("namespace1").build()).build();
        Namespace ns4 = Namespace.builder().metadata(Metadata.builder().name("ns3").build()).build();
        Namespace ns5 = Namespace.builder().metadata(Metadata.builder().name("namespace2").build()).build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of(new ManagedClusterProperties("local")));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(ns1, ns2, ns3, ns4, ns5));

        assertEquals(List.of(ns1, ns2, ns4), namespaceService.findByWildcardName("ns?"));
    }

    @Test
    void listNamespacesWithSuffixWildcardNameParameter() {
        Namespace ns1 = Namespace.builder().metadata(Metadata.builder().name("ns1").build()).build();
        Namespace ns2 = Namespace.builder().metadata(Metadata.builder().name("ns2").build()).build();
        Namespace ns3 = Namespace.builder().metadata(Metadata.builder().name("namespace1").build()).build();
        Namespace ns4 = Namespace.builder().metadata(Metadata.builder().name("ns3").build()).build();
        Namespace ns5 = Namespace.builder().metadata(Metadata.builder().name("namespace2").build()).build();

        when(managedClusterProperties.stream()).thenReturn(Stream.of(new ManagedClusterProperties("local")));
        when(namespaceRepository.findAllForCluster("local")).thenReturn(List.of(ns1, ns2, ns3, ns4, ns5));

        assertEquals(List.of(ns2, ns5), namespaceService.findByWildcardName("*2"));
    }

    @Test
    void listAllNamespaceResourcesEmpty() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(roleBindingService.findAllForNamespace("namespace"))
            .thenReturn(List.of());
        when(aclService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectClusterService.findAllByNamespaceWithOwnerPermission(ns))
            .thenReturn(List.of());
        when(resourceQuotaService.findByNamespace("namespace"))
            .thenReturn(Optional.empty());

        List<String> result = namespaceService.listAllNamespaceResources(ns);
        assertTrue(result.isEmpty());
    }

    @Test
    void listAllNamespaceResourcesTopic() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic));
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(roleBindingService.findAllForNamespace("namespace"))
            .thenReturn(List.of());
        when(aclService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectClusterService.findAllByNamespaceWithOwnerPermission(ns))
            .thenReturn(List.of());
        when(resourceQuotaService.findByNamespace("namespace"))
            .thenReturn(Optional.empty());

        List<String> result = namespaceService.listAllNamespaceResources(ns);
        assertEquals(1, result.size());
        assertEquals("Topic/topic", result.getFirst());
    }

    @Test
    void listAllNamespaceResourcesConnect() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        Connector connector = Connector.builder()
            .metadata(Metadata.builder()
                .name("connector")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of(connector));
        when(roleBindingService.findAllForNamespace("namespace"))
            .thenReturn(List.of());
        when(aclService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectClusterService.findAllByNamespaceWithOwnerPermission(ns))
            .thenReturn(List.of());
        when(resourceQuotaService.findByNamespace("namespace"))
            .thenReturn(Optional.empty());

        List<String> result = namespaceService.listAllNamespaceResources(ns);
        assertEquals(1, result.size());
        assertEquals("Connector/connector", result.getFirst());
    }

    @Test
    void listAllNamespaceResourcesRoleBinding() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        RoleBinding rb = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("rolebinding")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(roleBindingService.findAllForNamespace("namespace"))
            .thenReturn(List.of(rb));
        when(aclService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectClusterService.findAllByNamespaceWithOwnerPermission(ns))
            .thenReturn(List.of());
        when(resourceQuotaService.findByNamespace("namespace"))
            .thenReturn(Optional.empty());

        List<String> result = namespaceService.listAllNamespaceResources(ns);
        assertEquals(1, result.size());
        assertEquals("RoleBinding/rolebinding", result.getFirst());
    }

    @Test
    void listAllNamespaceResourcesAccessControlEntry() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        AccessControlEntry ace = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(roleBindingService.findAllForNamespace("namespace"))
            .thenReturn(List.of());
        when(aclService.findAllForNamespace(ns))
            .thenReturn(List.of(ace));
        when(connectClusterService.findAllByNamespaceWithOwnerPermission(ns))
            .thenReturn(List.of());
        when(resourceQuotaService.findByNamespace("namespace"))
            .thenReturn(Optional.empty());

        List<String> result = namespaceService.listAllNamespaceResources(ns);
        assertEquals(1, result.size());
        assertEquals("AccessControlEntry/ace", result.getFirst());
    }

    @Test
    void listAllNamespaceResourcesConnectCluster() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder()
                .name("connect-cluster")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(roleBindingService.findAllForNamespace("namespace"))
            .thenReturn(List.of());
        when(aclService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectClusterService.findAllByNamespaceWithOwnerPermission(ns))
            .thenReturn(List.of(connectCluster));
        when(resourceQuotaService.findByNamespace("namespace"))
            .thenReturn(Optional.empty());

        List<String> result = namespaceService.listAllNamespaceResources(ns);
        assertEquals(1, result.size());
        assertEquals("ConnectCluster/connect-cluster", result.getFirst());
    }

    @Test
    void listAllNamespaceResourcesQuota() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .name("resource-quota")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(roleBindingService.findAllForNamespace("namespace"))
            .thenReturn(List.of());
        when(aclService.findAllForNamespace(ns))
            .thenReturn(List.of());
        when(connectClusterService.findAllByNamespaceWithOwnerPermission(ns))
            .thenReturn(List.of());
        when(resourceQuotaService.findByNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));

        List<String> result = namespaceService.listAllNamespaceResources(ns);
        assertEquals(1, result.size());
        assertEquals("ResourceQuota/resource-quota", result.getFirst());
    }

    @Test
    void shouldDelete() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .kafkaUser("user")
                .build())
            .build();

        namespaceService.delete(ns);

        verify(namespaceRepository).delete(ns);
    }
}
