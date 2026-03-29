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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.NamespaceService;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AkhqControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    AclService aclService;

    @Mock
    Ns4KafkaProperties ns4KafkaProperties;

    @InjectMocks
    AkhqController akhqController;

    @Test
    void shouldComputeAllowedRegexEmpty() {
        List<AccessControlEntry> inputAcls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project2.topic1")
                                .build())
                        .build());

        List<String> actual = akhqController.computeAllowedRegexListForResourceType(
                inputAcls, AccessControlEntry.ResourceType.CONNECT);

        assertEquals(1, actual.size());
        assertEquals("^none$", actual.getFirst());
    }

    @Test
    void shouldComputeAllowedRegexList() {
        List<AccessControlEntry> inputAcls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project2.topic1")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.connects")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.GROUP)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build());

        List<String> actual =
                akhqController.computeAllowedRegexListForResourceType(inputAcls, AccessControlEntry.ResourceType.TOPIC);

        assertEquals(2, actual.size());
        assertLinesMatch(List.of("^\\Qproject1.\\E.*$", "^\\Qproject2.topic1\\E$"), actual);

        assertFalse(actual.contains("^\\Qproject1.connects\\E.*$"));
    }

    @Test
    void shouldComputeAllowedRegexListWithDistinct() {
        List<AccessControlEntry> inputAcls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build());

        List<String> actual =
                akhqController.computeAllowedRegexListForResourceType(inputAcls, AccessControlEntry.ResourceType.TOPIC);

        assertEquals(1, actual.size());
        assertLinesMatch(List.of("^\\Qproject1.\\E.*$"), actual);
    }

    @Test
    void shouldGenerateClaimV2() {
        Namespace ns1 = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();
        Namespace ns2 = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns2")
                        .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                        .build())
                .build();
        Namespace ns3 = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns3")
                        .labels(Map.of("support-group", "GP-PROJECT2-SUPPORT"))
                        .build())
                .build();
        Namespace ns4 = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns4")
                        .labels(Map.of("other-key", "anything"))
                        .build())
                .build();
        Namespace ns5 = Namespace.builder()
                .metadata(Resource.Metadata.builder().name("ns5").build())
                .build();

        AccessControlEntry ns1Ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();
        AccessControlEntry ns1Ace2 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_c.")
                        .build())
                .build();
        AccessControlEntry ns2Ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project2_t.")
                        .build())
                .build();
        AccessControlEntry ns2Ace2 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.") // ACL granted by ns1 to ns2
                        .build())
                .build();
        AccessControlEntry ns3Ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resource("project3_topic")
                        .build())
                .build();
        AccessControlEntry pubAce1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("public_t.")
                        .build())
                .build();

        when(ns4KafkaProperties.getAkhq()).thenReturn(buildAkhqProperties());
        when(namespaceService.findAll()).thenReturn(List.of(ns1, ns2, ns3, ns4, ns5));
        when(aclService.findAllGrantedToNamespace(ns1)).thenReturn(List.of(ns1Ace1, ns1Ace2, pubAce1));
        when(aclService.findAllGrantedToNamespace(ns2)).thenReturn(List.of(ns2Ace1, ns2Ace2, pubAce1));
        when(aclService.findAllGrantedToNamespace(ns3)).thenReturn(List.of(ns3Ace1, pubAce1));

        AkhqController.AkhqClaimRequest request = AkhqController.AkhqClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT", "GP-PROJECT2-SUPPORT"))
                .build();

        AkhqController.AkhqClaimResponseV2 actual = akhqController.generateClaimV2(request);

        assertLinesMatch(
                List.of(
                        "topic/read",
                        "topic/data/read",
                        "group/read",
                        "registry/read",
                        "connect/read",
                        "connect/state/update"),
                actual.getRoles());
        assertEquals(4, actual.getTopicsFilterRegexp().size());
        assertLinesMatch(
                List.of(
                        "^\\Qproject1_t.\\E.*$",
                        "^\\Qpublic_t.\\E.*$",
                        "^\\Qproject2_t.\\E.*$",
                        "^\\Qproject3_topic\\E$"),
                actual.getTopicsFilterRegexp());

        verify(aclService).findAllGrantedToNamespace(ns1);
        verify(aclService).findAllGrantedToNamespace(ns2);
        verify(aclService).findAllGrantedToNamespace(ns3);
        verify(aclService, never()).findAllGrantedToNamespace(ns4);
        verify(aclService, never()).findAllGrantedToNamespace(ns5);
        verify(aclService).findAllPublicGrantedTo();
    }

    @Test
    void shouldGenerateClaimV2WhenNullOrEmptyRequest() {
        when(ns4KafkaProperties.getAkhq()).thenReturn(buildAkhqProperties());

        AkhqController.AkhqClaimResponseV2 actual = akhqController.generateClaimV2(null);

        assertEquals(1, actual.getTopicsFilterRegexp().size());
        assertEquals("^none$", actual.getTopicsFilterRegexp().getFirst());

        AkhqController.AkhqClaimRequest request =
                AkhqController.AkhqClaimRequest.builder().build();
        actual = akhqController.generateClaimV2(request);

        assertEquals(1, actual.getTopicsFilterRegexp().size());
        assertEquals("^none$", actual.getTopicsFilterRegexp().getFirst());

        request = AkhqController.AkhqClaimRequest.builder().groups(List.of()).build();
        actual = akhqController.generateClaimV2(request);

        assertEquals(1, actual.getTopicsFilterRegexp().size());
        assertEquals("^none$", actual.getTopicsFilterRegexp().getFirst());

        assertLinesMatch(
                List.of(
                        "topic/read",
                        "topic/data/read",
                        "group/read",
                        "registry/read",
                        "connect/read",
                        "connect/state/update"),
                actual.getRoles());
    }

    @Test
    void shouldGenerateClaimV2ForAdmin() {
        when(ns4KafkaProperties.getAkhq()).thenReturn(buildAkhqProperties());

        AkhqController.AkhqClaimRequest request = AkhqController.AkhqClaimRequest.builder()
                .groups(List.of("GP-ADMIN"))
                .build();

        AkhqController.AkhqClaimResponseV2 actual = akhqController.generateClaimV2(request);
        // AdminRoles
        assertLinesMatch(
                List.of(
                        "topic/read",
                        "topic/data/read",
                        "group/read",
                        "registry/read",
                        "connect/read",
                        "connect/state/update"),
                actual.getRoles());
        // Admin Regexp
        assertLinesMatch(List.of(".*$"), actual.getTopicsFilterRegexp());
        assertLinesMatch(List.of(".*$"), actual.getConnectsFilterRegexp());
        assertLinesMatch(List.of(".*$"), actual.getConsumerGroupsFilterRegexp());
    }

    @Test
    void shouldComputeAllowedRegexListFilterStartWith() {
        List<AccessControlEntry> inputAcls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project1.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project1.topic1")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project2.topic2")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project2.topic3")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project3.topic4")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resource("project3.topic5")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resource("project3.")
                                .build())
                        .build());

        List<String> actual =
                akhqController.computeAllowedRegexListForResourceType(inputAcls, AccessControlEntry.ResourceType.TOPIC);

        assertEquals(4, actual.size());
        assertLinesMatch(
                List.of(
                        "^\\Qproject1.\\E.*$",
                        "^\\Qproject2.topic2\\E$",
                        "^\\Qproject2.topic3\\E$",
                        "^\\Qproject3.\\E.*$"),
                actual);
    }

    private Ns4KafkaProperties.AkhqProperties buildAkhqProperties() {
        Ns4KafkaProperties.AkhqProperties akhqProperties = new Ns4KafkaProperties.AkhqProperties();
        akhqProperties.setGroupLabel("support-group");
        akhqProperties.setFormerRoles(List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"));
        akhqProperties.setAdminGroup("GP-ADMIN");
        akhqProperties.setFormerAdminRoles(List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"));
        return akhqProperties;
    }
}
