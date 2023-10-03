package com.michelin.ns4kafka.controllers;

import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.properties.AkhqProperties;
import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.NamespaceService;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AkhqClaimProviderControllerV3Test {
    @Mock
    NamespaceService namespaceService;

    @Mock
    AccessControlEntryService accessControlEntryService;

    @InjectMocks
    AkhqClaimProviderController akhqClaimProviderController;

    @Spy
    AkhqProperties akhqProperties = getAkhqClaimProviderControllerConfig();

    private AkhqProperties getAkhqClaimProviderControllerConfig() {
        AkhqProperties config = new AkhqProperties();
        config.setGroupLabel("support-group");
        config.setAdminGroup("GP-ADMIN");
        config.setRoles(Map.of(AccessControlEntry.ResourceType.TOPIC, "topic-read",
            AccessControlEntry.ResourceType.CONNECT, "connect-rw",
            AccessControlEntry.ResourceType.SCHEMA, "registry-read"));
        config.setAdminRoles(Map.of(AccessControlEntry.ResourceType.TOPIC, "topic-admin",
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

        akhqClaimProviderController.managedClusters =
            List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));
        when(namespaceService.listAll())
            .thenReturn(List.of(ns1Cluster1));
        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1))
            .thenReturn(List.of(ace1Ns1Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        Assertions.assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void generateClaimMultipleSupportGroups() {
        Namespace ns1Cluster1 = Namespace.builder()
            .metadata(ObjectMeta.builder().name("ns1").cluster("cluster1")
                .labels(Map.of("support-group", "GP-PROJECT1-DEV,GP-PROJECT1-SUPPORT,GP-PROJECT1-OPS"))
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

        akhqClaimProviderController.managedClusters =
            List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));
        when(namespaceService.listAll())
            .thenReturn(List.of(ns1Cluster1));
        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1))
            .thenReturn(List.of(ace1Ns1Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups = actual.getGroups().get("group");
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

        akhqClaimProviderController.managedClusters =
            List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));
        when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT2-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);
        Assertions.assertNull(actual.getGroups());
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

        akhqClaimProviderController.managedClusters =
            List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));
        when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1, ns1Cluster2));
        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1))
            .thenReturn(List.of(ace1Ns1Cluster1));

        AccessControlEntry ace1Ns1Cluster2 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder().cluster("cluster2").build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .resource("project1_t.")
                .build())
            .build();

        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster2))
            .thenReturn(List.of(ace1Ns1Cluster2));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups = actual.getGroups().get("group");
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

        akhqClaimProviderController.managedClusters =
            List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));
        when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1, ns2Cluster1));
        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1))
            .thenReturn(List.of(ace1Ns1Cluster1));

        AccessControlEntry ace2Ns2Cluster1 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder().cluster("cluster1").build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .resource("project2_t.")
                .build())
            .build();

        when(accessControlEntryService.findAllGrantedToNamespace(ns2Cluster1))
            .thenReturn(List.of(ace2Ns2Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1&2-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups = actual.getGroups().get("group");
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

        akhqClaimProviderController.managedClusters =
            List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));
        when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1, ns1Cluster2));
        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1))
            .thenReturn(List.of(ace1Cluster1));

        AccessControlEntry ace1Cluster2 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder().cluster("cluster2").build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .resource("project1_t.")
                .build())
            .build();

        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster2))
            .thenReturn(List.of(ace1Cluster2));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups = actual.getGroups().get("group");
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

        akhqClaimProviderController.managedClusters =
            List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));
        when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1, ns2Cluster2));
        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1))
            .thenReturn(List.of(ace1Ns1Cluster1));

        AccessControlEntry ace1Ns2Cluster2 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder().cluster("cluster2").build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .resource("project2_t.")
                .build())
            .build();

        when(accessControlEntryService.findAllGrantedToNamespace(ns2Cluster2))
            .thenReturn(List.of(ace1Ns2Cluster2));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1&2-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        Assertions.assertEquals(actual.getGroups().size(), 1);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(4, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        Assertions.assertEquals("topic-read", groups.get(1).getRole());
        Assertions.assertEquals(List.of("^\\Qproject2_t.\\E.*$"), groups.get(1).getPatterns());
        Assertions.assertEquals(List.of("^cluster2$"), groups.get(1).getClusters());
        Assertions.assertEquals("registry-read", groups.get(2).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(2).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$"), groups.get(2).getClusters());
        Assertions.assertEquals("registry-read", groups.get(3).getRole());
        Assertions.assertEquals(List.of("^\\Qproject2_t.\\E.*$"), groups.get(3).getPatterns());
        Assertions.assertEquals(List.of("^cluster2$"), groups.get(3).getClusters());
    }

    @Test
    void generateClaimAndOptimizePatterns() {
        Namespace ns1Cluster1 = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1").cluster("cluster1").labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                .build())
            .build();

        List<AccessControlEntry> inputAcls = List.of(
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project1.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resource("project1.topic1")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.CONNECT)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resource("project1.topic1")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resource("project2.topic2")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resource("project2.topic2a")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resource("project2.topic3")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.CONNECT)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project2.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resource("project3.topic4")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                    .resource("project3.topic5")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project3.")
                    .build())
                .build()
        );
        akhqClaimProviderController.managedClusters =
            List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));
        when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1));
        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(inputAcls);

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1&2-SUPPORT"))
            .build();
        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(3, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(
            List.of("^\\Qproject1.\\E.*$", "^\\Qproject2.topic2\\E$", "^\\Qproject2.topic2a\\E$",
                "^\\Qproject2.topic3\\E$", "^\\Qproject3.\\E.*$"),
            groups.get(0).getPatterns()
        );
        Assertions.assertEquals("connect-rw", groups.get(1).getRole());
        Assertions.assertEquals(
            List.of("^\\Qproject1.topic1\\E$", "^\\Qproject2.\\E.*$"),
            groups.get(1).getPatterns()
        );
        Assertions.assertEquals("registry-read", groups.get(2).getRole());
        Assertions.assertEquals(
            List.of("^\\Qproject1.\\E.*$", "^\\Qproject3.\\E.*$", "^\\Qproject2.topic2-\\E(key|value)$",
                    "^\\Qproject2.topic2a-\\E(key|value)$", "^\\Qproject2.topic3-\\E(key|value)$"),
            groups.get(2).getPatterns()
        );
    }

    @Test
    void generateClaimAndOptimizePatternsForDifferentClusters() {
        Namespace ns1Cluster1 = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("ns1").cluster("cluster1").labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                .build())
            .build();

        List<AccessControlEntry> inputAcls = List.of(
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project1.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project1.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project2.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project3.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project3.")
                    .build())
                .build(),
            AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().cluster("cluster3").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .resource("project3.")
                    .build())
                .build()
        );
        akhqClaimProviderController.managedClusters = List.of(new ManagedClusterProperties("cluster1"),
            new ManagedClusterProperties("cluster2"), new ManagedClusterProperties("cluster3"),
            new ManagedClusterProperties("cluster4"));
        when(namespaceService.listAll()).thenReturn(List.of(ns1Cluster1));
        when(accessControlEntryService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(inputAcls);

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1&2-SUPPORT"))
            .build();
        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups = actual.getGroups().get("group");
        Assertions.assertEquals(6, groups.size());
        Assertions.assertEquals("topic-read", groups.get(0).getRole());
        Assertions.assertEquals(List.of("^\\Qproject1.\\E.*$"), groups.get(0).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$", "^cluster2$"), groups.get(0).getClusters());
        Assertions.assertEquals("topic-read", groups.get(1).getRole());
        Assertions.assertEquals(List.of("^\\Qproject2.\\E.*$"), groups.get(1).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$"), groups.get(1).getClusters());
        Assertions.assertEquals("topic-read", groups.get(2).getRole());
        Assertions.assertEquals(List.of("^\\Qproject3.\\E.*$"), groups.get(2).getPatterns());
        Assertions.assertEquals(List.of("^cluster1$", "^cluster2$", "^cluster3$"), groups.get(2).getClusters());
    }
}
