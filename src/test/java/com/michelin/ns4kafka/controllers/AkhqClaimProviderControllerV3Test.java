package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.config.AkhqClaimProviderControllerConfig;
import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.NamespaceService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class AkhqClaimProviderControllerV3Test {
    @Mock
    NamespaceService namespaceService;

    @Mock
    AccessControlEntryService accessControlEntryService;

    @InjectMocks
    AkhqClaimProviderController akhqClaimProviderController;

    @Spy
    AkhqClaimProviderControllerConfig akhqClaimProviderControllerConfig = getAkhqClaimProviderControllerConfig();

    private AkhqClaimProviderControllerConfig getAkhqClaimProviderControllerConfig() {
        AkhqClaimProviderControllerConfig config = new AkhqClaimProviderControllerConfig();
        config.setGroupLabel("support-group");
        config.setAdminGroup("GP-ADMIN");
        config.setNewRoles(Map.of(AccessControlEntry.ResourceType.TOPIC, "topic-read",
                AccessControlEntry.ResourceType.CONNECT, "connect-rw",
                AccessControlEntry.ResourceType.SCHEMA, "registry-read"));
        config.setNewAdminRoles(Map.of(AccessControlEntry.ResourceType.TOPIC, "topic-admin",
                AccessControlEntry.ResourceType.CONNECT, "connect-admin",
                AccessControlEntry.ResourceType.SCHEMA, "registry-admin"));
        return config;
    }

    @Test
    void generateClaimHappyPath() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(ObjectMeta.builder().name("ns1").cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters = List.of(new KafkaAsyncExecutorConfig("cluster1"), new KafkaAsyncExecutorConfig("cluster2"));
        Mockito.when(namespaceService.listAll())
                .thenReturn(List.of(ns1Cluster1));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1))
                .thenReturn(List.of(ace1Ns1Cluster1));

        AkhqClaimProviderController.AKHQClaimRequest request = AkhqClaimProviderController.AKHQClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT"))
                .build();

        AkhqClaimProviderController.AKHQClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AKHQClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        Assertions.assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void generateClaimNoPermissions() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(ObjectMeta.builder().name("ns1").cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        akhqClaimProviderController.managedClusters = List.of(new KafkaAsyncExecutorConfig("cluster1"), new KafkaAsyncExecutorConfig("cluster2"));
        Mockito.when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1));

        AkhqClaimProviderController.AKHQClaimRequest request = AkhqClaimProviderController.AKHQClaimRequest.builder()
                .groups(List.of("GP-PROJECT2-SUPPORT"))
                .build();

        AkhqClaimProviderController.AKHQClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        List<AkhqClaimProviderController.AKHQClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals("registry-read", groups.get(0).getRole());
    }

    @Test
    void generateClaimWithOptimizedClusters() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1").cluster("cluster1").labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();
        Namespace ns1Cluster2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1").cluster("cluster2").labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster2 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters = List.of(new KafkaAsyncExecutorConfig("cluster1"), new KafkaAsyncExecutorConfig("cluster2"));
        Mockito.when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1, ns1Cluster2));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster2)).thenReturn(List.of(ace1Ns1Cluster2));

        AkhqClaimProviderController.AKHQClaimRequest request = AkhqClaimProviderController.AKHQClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT"))
                .build();

        AkhqClaimProviderController.AKHQClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AKHQClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        Assertions.assertEquals(List.of("^.*$"), groups.get(0).getClusters());
        Assertions.assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void generateClaimWithMultiplePatternsOnSameCluster() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1").cluster("cluster1").labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();
        Namespace ns2Cluster1 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns2").cluster("cluster1").labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        AccessControlEntry ace2Ns2Cluster1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project2_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters = List.of(new KafkaAsyncExecutorConfig("cluster1"), new KafkaAsyncExecutorConfig("cluster2"));
        Mockito.when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1, ns2Cluster1));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns2Cluster1)).thenReturn(List.of(ace2Ns2Cluster1));

        AkhqClaimProviderController.AKHQClaimRequest request = AkhqClaimProviderController.AKHQClaimRequest.builder()
                .groups(List.of("GP-PROJECT1&2-SUPPORT"))
                .build();

        AkhqClaimProviderController.AKHQClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AKHQClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1_t.\\E.*$", "^\\Qproject2_t.\\E.*$"), groups.get(0).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        Assertions.assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void generateClaimWithMultipleGroups() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1").cluster("cluster1").labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();
        Namespace ns1Cluster2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1").cluster("cluster2").labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Cluster1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        AccessControlEntry ace1Cluster2 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters = List.of(new KafkaAsyncExecutorConfig("cluster1"), new KafkaAsyncExecutorConfig("cluster2"));
        Mockito.when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1, ns1Cluster2));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Cluster1));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster2)).thenReturn(List.of(ace1Cluster2));

        AkhqClaimProviderController.AKHQClaimRequest request = AkhqClaimProviderController.AKHQClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT"))
                .build();

        AkhqClaimProviderController.AKHQClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AKHQClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        Assertions.assertEquals(List.of("^.*$"), groups.get(0).getClusters());
        Assertions.assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void generateClaimWithPatternOnMultipleClusters() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1").cluster("cluster1").labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();
        Namespace ns2Cluster2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns2").cluster("cluster2").labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        AccessControlEntry ace1Ns2Cluster2 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project2_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters = List.of(new KafkaAsyncExecutorConfig("cluster1"), new KafkaAsyncExecutorConfig("cluster2"));
        Mockito.when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1, ns2Cluster2));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns2Cluster2)).thenReturn(List.of(ace1Ns2Cluster2));

        AkhqClaimProviderController.AKHQClaimRequest request = AkhqClaimProviderController.AKHQClaimRequest.builder()
                .groups(List.of("GP-PROJECT1&2-SUPPORT"))
                .build();

        AkhqClaimProviderController.AKHQClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AKHQClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(3, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        Assertions.assertEquals("topic-read", groups.get(1).getRole());
        Assertions.assertEquals(List.of("^\\Qproject2_t.\\E.*$"), groups.get(1).getPatterns());
        Assertions.assertEquals(List.of("^cluster2$"), groups.get(1).getClusters());
        Assertions.assertEquals("registry-read", groups.get(2).getRole());
    }
}
