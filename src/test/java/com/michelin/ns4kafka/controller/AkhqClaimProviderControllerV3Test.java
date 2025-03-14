/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.property.AkhqProperties;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.NamespaceService;
import java.util.List;
import java.util.Map;
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
    AclService aclService;

    @InjectMocks
    AkhqClaimProviderController akhqClaimProviderController;

    @Spy
    AkhqProperties akhqProperties = getAkhqClaimProviderControllerConfig();

    @Test
    void shouldGenerateClaimHappyPath() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT"))
                .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(2, groups.size());
        assertEquals("topic-read", groups.get(0).getRole());
        assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void shouldGrantAllAccessToGroup() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        AccessControlEntry ace2Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.GROUP)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1, ace2Ns1Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT"))
                .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(3, groups.size());
        assertEquals("topic-read", groups.get(0).getRole());
        assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        assertEquals("group-read", groups.get(1).getRole());
        assertEquals("registry-read", groups.get(2).getRole());
    }

    @Test
    void shouldGenerateClaimWithMultipleSupportGroups() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1-DEV,GP-PROJECT1-SUPPORT,GP-PROJECT1-OPS"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT"))
                .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(2, groups.size());
        assertEquals("topic-read", groups.get(0).getRole());
        assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void shouldGenerateClaimNoPermissions() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT2-SUPPORT"))
                .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);
        assertNull(actual.getGroups());
    }

    @Test
    void shouldGenerateClaimWithOptimizedClusters() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        Namespace ns1Cluster2 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster2")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1, ns1Cluster2));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1));

        AccessControlEntry ace1Ns1Cluster2 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        when(aclService.findAllGrantedToNamespace(ns1Cluster2)).thenReturn(List.of(ace1Ns1Cluster2));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT"))
                .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(2, groups.size());
        assertEquals("topic-read", groups.get(0).getRole());
        assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        assertEquals(List.of("^.*$"), groups.get(0).getClusters());
        assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void shouldGenerateClaimWithMultiplePatternsOnSameCluster() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();

        Namespace ns2Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns2")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1, ns2Cluster1));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1));

        AccessControlEntry ace2Ns2Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project2_t.")
                        .build())
                .build();

        when(aclService.findAllGrantedToNamespace(ns2Cluster1)).thenReturn(List.of(ace2Ns2Cluster1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1&2-SUPPORT"))
                .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(2, groups.size());
        assertEquals("topic-read", groups.get(0).getRole());
        assertEquals(
                List.of("^\\Qproject1_t.\\E.*$", "^\\Qproject2_t.\\E.*$"),
                groups.get(0).getPatterns());
        assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void shouldGenerateClaimWithMultipleGroups() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        Namespace ns1Cluster2 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster2")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1, ns1Cluster2));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Cluster1));

        AccessControlEntry ace1Cluster2 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        when(aclService.findAllGrantedToNamespace(ns1Cluster2)).thenReturn(List.of(ace1Cluster2));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT"))
                .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(2, groups.size());
        assertEquals("topic-read", groups.get(0).getRole());
        assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        assertEquals(List.of("^.*$"), groups.get(0).getClusters());
        assertEquals("registry-read", groups.get(1).getRole());
    }

    @Test
    void shouldGenerateClaimWithPatternOnMultipleClusters() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();

        Namespace ns2Cluster2 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns2")
                        .cluster("cluster2")
                        .labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();

        AccessControlEntry ace1Ns1Cluster1 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1, ns2Cluster2));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(List.of(ace1Ns1Cluster1));

        AccessControlEntry ace1Ns2Cluster2 = AccessControlEntry.builder()
                .metadata(Metadata.builder().cluster("cluster2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project2_t.")
                        .build())
                .build();

        when(aclService.findAllGrantedToNamespace(ns2Cluster2)).thenReturn(List.of(ace1Ns2Cluster2));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1&2-SUPPORT"))
                .build();

        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        assertEquals(1, actual.getGroups().size());

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(4, groups.size());
        assertEquals("topic-read", groups.get(0).getRole());
        assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(0).getPatterns());
        assertEquals(List.of("^cluster1$"), groups.get(0).getClusters());
        assertEquals("topic-read", groups.get(1).getRole());
        assertEquals(List.of("^\\Qproject2_t.\\E.*$"), groups.get(1).getPatterns());
        assertEquals(List.of("^cluster2$"), groups.get(1).getClusters());
        assertEquals("registry-read", groups.get(2).getRole());
        assertEquals(List.of("^\\Qproject1_t.\\E.*$"), groups.get(2).getPatterns());
        assertEquals(List.of("^cluster1$"), groups.get(2).getClusters());
        assertEquals("registry-read", groups.get(3).getRole());
        assertEquals(List.of("^\\Qproject2_t.\\E.*$"), groups.get(3).getPatterns());
        assertEquals(List.of("^cluster2$"), groups.get(3).getClusters());
    }

    @Test
    void shouldGenerateClaimAndOptimizePatterns() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();

        List<AccessControlEntry> inputAcls = List.of(
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project1.topic1")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project1.topic1")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project2.topic2")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project2.topic2a")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project2.topic3")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project2.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project3.topic4")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project3.topic5")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project3.")
                                .build())
                        .build());

        akhqClaimProviderController.managedClusters =
                List.of(new ManagedClusterProperties("cluster1"), new ManagedClusterProperties("cluster2"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(inputAcls);

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1&2-SUPPORT"))
                .build();
        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(3, groups.size());
        assertEquals("topic-read", groups.getFirst().getRole());
        assertEquals(
                List.of(
                        "^\\Qproject1.\\E.*$",
                        "^\\Qproject2.topic2\\E$",
                        "^\\Qproject2.topic2a\\E$",
                        "^\\Qproject2.topic3\\E$",
                        "^\\Qproject3.\\E.*$"),
                groups.getFirst().getPatterns());
        assertEquals("connect-rw", groups.get(1).getRole());
        assertEquals(
                List.of("^\\Qproject1.topic1\\E$", "^\\Qproject2.\\E.*$"),
                groups.get(1).getPatterns());
        assertEquals("registry-read", groups.get(2).getRole());
        assertEquals(
                List.of(
                        "^\\Qproject1.\\E.*$",
                        "^\\Qproject3.\\E.*$",
                        "^\\Qproject2.topic2-\\E(key|value)$",
                        "^\\Qproject2.topic2a-\\E(key|value)$",
                        "^\\Qproject2.topic3-\\E(key|value)$"),
                groups.get(2).getPatterns());
    }

    @Test
    void shouldGenerateClaimAndOptimizePatternsForDifferentClusters() {
        Namespace ns1Cluster1 = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("ns1")
                        .cluster("cluster1")
                        .labels(Map.of("support-group", "GP-PROJECT1&2-SUPPORT"))
                        .build())
                .build();

        List<AccessControlEntry> inputAcls = List.of(
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster2").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project2.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster1").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project3.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster2").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project3.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .metadata(Metadata.builder().cluster("cluster3").build())
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project3.")
                                .build())
                        .build());
        akhqClaimProviderController.managedClusters = List.of(
                new ManagedClusterProperties("cluster1"),
                new ManagedClusterProperties("cluster2"),
                new ManagedClusterProperties("cluster3"),
                new ManagedClusterProperties("cluster4"));

        when(namespaceService.findAll()).thenReturn(List.of(ns1Cluster1));
        when(aclService.findAllGrantedToNamespace(ns1Cluster1)).thenReturn(inputAcls);

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1&2-SUPPORT"))
                .build();
        AkhqClaimProviderController.AkhqClaimResponseV3 actual = akhqClaimProviderController.generateClaimV3(request);

        List<AkhqClaimProviderController.AkhqClaimResponseV3.Group> groups =
                actual.getGroups().get("group");
        assertEquals(6, groups.size());
        assertEquals("topic-read", groups.get(0).getRole());
        assertEquals(List.of("^\\Qproject1.\\E.*$"), groups.get(0).getPatterns());
        assertEquals(List.of("^cluster1$", "^cluster2$"), groups.get(0).getClusters());
        assertEquals("topic-read", groups.get(1).getRole());
        assertEquals(List.of("^\\Qproject2.\\E.*$"), groups.get(1).getPatterns());
        assertEquals(List.of("^cluster1$"), groups.get(1).getClusters());
        assertEquals("topic-read", groups.get(2).getRole());
        assertEquals(List.of("^\\Qproject3.\\E.*$"), groups.get(2).getPatterns());
        assertEquals(
                List.of("^cluster1$", "^cluster2$", "^cluster3$"), groups.get(2).getClusters());
    }

    private AkhqProperties getAkhqClaimProviderControllerConfig() {
        AkhqProperties config = new AkhqProperties();
        config.setGroupLabel("support-group");
        config.setAdminGroup("GP-ADMIN");
        config.setRoles(Map.of(
                AccessControlEntry.ResourceType.TOPIC,
                "topic-read",
                AccessControlEntry.ResourceType.CONNECT,
                "connect-rw",
                AccessControlEntry.ResourceType.SCHEMA,
                "registry-read",
                AccessControlEntry.ResourceType.GROUP,
                "group-read"));
        config.setAdminRoles(Map.of(
                AccessControlEntry.ResourceType.TOPIC,
                "topic-admin",
                AccessControlEntry.ResourceType.CONNECT,
                "connect-admin",
                AccessControlEntry.ResourceType.SCHEMA,
                "registry-admin",
                AccessControlEntry.ResourceType.GROUP,
                "group-read"));
        return config;
    }
}
