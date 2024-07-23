package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.property.AkhqProperties;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.NamespaceService;
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
class AkhqClaimProviderControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    AclService aclService;

    @InjectMocks
    AkhqClaimProviderController akhqClaimProviderController;

    @Spy
    AkhqProperties akhqProperties = getAkhqClaimProviderControllerConfig();

    private AkhqProperties getAkhqClaimProviderControllerConfig() {
        AkhqProperties config = new AkhqProperties();
        config.setGroupLabel("support-group");
        config.setFormerRoles(List.of(
            "topic/read",
            "topic/data/read",
            "group/read",
            "registry/read",
            "connect/read",
            "connect/state/update"
        ));
        config.setAdminGroup("GP-ADMIN");
        config.setFormerAdminRoles(List.of(
            "topic/read",
            "topic/data/read",
            "group/read",
            "registry/read",
            "connect/read",
            "connect/state/update"
        ));
        return config;
    }

    @Test
    void computeAllowedRegexListTestEmpty() {
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
                .build()
        );

        List<String> actual = akhqClaimProviderController.computeAllowedRegexListForResourceType(inputAcls,
            AccessControlEntry.ResourceType.CONNECT);

        assertEquals(1, actual.size());
        assertEquals("^none$", actual.get(0));
    }

    @Test
    void computeAllowedRegexListTestSuccess() {
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
                .build()
        );
        List<String> actual = akhqClaimProviderController.computeAllowedRegexListForResourceType(inputAcls,
            AccessControlEntry.ResourceType.TOPIC);

        assertEquals(2, actual.size());
        assertLinesMatch(
            List.of(
                "^\\Qproject1.\\E.*$",
                "^\\Qproject2.topic1\\E$"
            ),
            actual
        );
        Assertions.assertFalse(actual.contains("^\\Qproject1.connects\\E.*$"));
    }

    @Test
    void computeAllowedRegexListTestSuccessDistinct() {
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
                .build()
        );
        List<String> actual = akhqClaimProviderController.computeAllowedRegexListForResourceType(inputAcls,
            AccessControlEntry.ResourceType.TOPIC);

        assertEquals(1, actual.size());
        assertLinesMatch(
            List.of(
                "^\\Qproject1.\\E.*$"
            ),
            actual
        );
    }

    @Test
    void generateClaimTestNullOrEmptyRequest() {
        AkhqClaimProviderController.AkhqClaimResponse actual = akhqClaimProviderController.generateClaim(null);

        assertEquals(1, actual.getAttributes().get("topicsFilterRegexp").size());
        assertEquals("^none$", actual.getAttributes().get("topicsFilterRegexp").get(0));

        AkhqClaimProviderController.AkhqClaimRequest request =
            AkhqClaimProviderController.AkhqClaimRequest.builder().build();
        actual = akhqClaimProviderController.generateClaim(request);

        assertEquals(1, actual.getAttributes().get("topicsFilterRegexp").size());
        assertEquals("^none$", actual.getAttributes().get("topicsFilterRegexp").get(0));

        request = AkhqClaimProviderController.AkhqClaimRequest.builder().groups(List.of()).build();
        actual = akhqClaimProviderController.generateClaim(request);

        assertEquals(1, actual.getAttributes().get("topicsFilterRegexp").size());
        assertEquals("^none$", actual.getAttributes().get("topicsFilterRegexp").get(0));

        assertLinesMatch(
            List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"
            ),
            actual.getRoles()
        );
    }

    @Test
    void generateClaimTestSuccess() {
        Namespace ns1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                .build())
            .build();
        Namespace ns2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns2")
                .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                .build())
            .build();
        Namespace ns3 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns3")
                .labels(Map.of("support-group", "GP-PROJECT2-SUPPORT"))
                .build())
            .build();
        Namespace ns4 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns4")
                .labels(Map.of("other-key", "anything"))
                .build())
            .build();
        Namespace ns5 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns5")
                .build())
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

        when(namespaceService.listAll())
            .thenReturn(List.of(ns1, ns2, ns3, ns4, ns5));
        when(aclService.findAllGrantedToNamespace(ns1))
            .thenReturn(List.of(ns1Ace1, ns1Ace2, pubAce1));
        when(aclService.findAllGrantedToNamespace(ns2))
            .thenReturn(List.of(ns2Ace1, ns2Ace2, pubAce1));
        when(aclService.findAllGrantedToNamespace(ns3))
            .thenReturn(List.of(ns3Ace1, pubAce1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1-SUPPORT", "GP-PROJECT2-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponse actual = akhqClaimProviderController.generateClaim(request);

        assertLinesMatch(
            List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"
            ), actual.getRoles());

        assertEquals(4, actual.getAttributes().get("topicsFilterRegexp").size());
        assertLinesMatch(
            List.of(
                "^\\Qproject1_t.\\E.*$",
                "^\\Qpublic_t.\\E.*$",
                "^\\Qproject2_t.\\E.*$",
                "^\\Qproject3_topic\\E$"
            ),
            actual.getAttributes().get("topicsFilterRegexp")
        );

        verify(aclService).findAllGrantedToNamespace(ns1);
        verify(aclService).findAllGrantedToNamespace(ns2);
        verify(aclService).findAllGrantedToNamespace(ns3);
        verify(aclService, never()).findAllGrantedToNamespace(ns4);
        verify(aclService, never()).findAllGrantedToNamespace(ns5);
        verify(aclService).findAllPublicGrantedTo();
    }

    @Test
    void generateClaimTestSuccessAdmin() {
        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-ADMIN"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponse actual = akhqClaimProviderController.generateClaim(request);
        assertLinesMatch(
            List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"
            ),
            actual.getRoles()
        );
        // Admin Regexp
        assertLinesMatch(List.of(".*$"), actual.getAttributes().get("topicsFilterRegexp"));
        assertLinesMatch(List.of(".*$"), actual.getAttributes().get("connectsFilterRegexp"));
        assertLinesMatch(List.of(".*$"), actual.getAttributes().get("consumerGroupsFilterRegexp"));
    }

    @Test
    void generateClaimV2TestSuccess() {
        Namespace ns1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                .build())
            .build();
        Namespace ns2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns2")
                .labels(Map.of("support-group", "GP-PROJECT1-SUPPORT"))
                .build())
            .build();
        Namespace ns3 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns3")
                .labels(Map.of("support-group", "GP-PROJECT2-SUPPORT"))
                .build())
            .build();
        Namespace ns4 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns4")
                .labels(Map.of("other-key", "anything"))
                .build())
            .build();
        Namespace ns5 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns5")
                .build())
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
        when(namespaceService.listAll())
            .thenReturn(List.of(ns1, ns2, ns3, ns4, ns5));
        when(aclService.findAllGrantedToNamespace(ns1))
            .thenReturn(List.of(ns1Ace1, ns1Ace2, pubAce1));
        when(aclService.findAllGrantedToNamespace(ns2))
            .thenReturn(List.of(ns2Ace1, ns2Ace2, pubAce1));
        when(aclService.findAllGrantedToNamespace(ns3))
            .thenReturn(List.of(ns3Ace1, pubAce1));

        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-PROJECT1-SUPPORT", "GP-PROJECT2-SUPPORT"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV2 actual = akhqClaimProviderController.generateClaimV2(request);

        assertLinesMatch(
            List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"
            ), actual.getRoles());
        assertEquals(4, actual.getTopicsFilterRegexp().size());
        assertLinesMatch(
            List.of(
                "^\\Qproject1_t.\\E.*$",
                "^\\Qpublic_t.\\E.*$",
                "^\\Qproject2_t.\\E.*$",
                "^\\Qproject3_topic\\E$"
            ),
            actual.getTopicsFilterRegexp()
        );

        verify(aclService).findAllGrantedToNamespace(ns1);
        verify(aclService).findAllGrantedToNamespace(ns2);
        verify(aclService).findAllGrantedToNamespace(ns3);
        verify(aclService, never()).findAllGrantedToNamespace(ns4);
        verify(aclService, never()).findAllGrantedToNamespace(ns5);
        verify(aclService).findAllPublicGrantedTo();
    }

    @Test
    void generateClaimV2TestNullOrEmptyRequest() {
        AkhqClaimProviderController.AkhqClaimResponseV2 actual = akhqClaimProviderController.generateClaimV2(null);

        assertEquals(1, actual.getTopicsFilterRegexp().size());
        assertEquals("^none$", actual.getTopicsFilterRegexp().get(0));

        AkhqClaimProviderController.AkhqClaimRequest request =
            AkhqClaimProviderController.AkhqClaimRequest.builder().build();
        actual = akhqClaimProviderController.generateClaimV2(request);

        assertEquals(1, actual.getTopicsFilterRegexp().size());
        assertEquals("^none$", actual.getTopicsFilterRegexp().get(0));

        request = AkhqClaimProviderController.AkhqClaimRequest.builder().groups(List.of()).build();
        actual = akhqClaimProviderController.generateClaimV2(request);

        assertEquals(1, actual.getTopicsFilterRegexp().size());
        assertEquals("^none$", actual.getTopicsFilterRegexp().get(0));

        assertLinesMatch(
            List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"
            ),
            actual.getRoles()
        );
    }

    @Test
    void generateClaimV2TestSuccessAdmin() {
        AkhqClaimProviderController.AkhqClaimRequest request = AkhqClaimProviderController.AkhqClaimRequest.builder()
            .groups(List.of("GP-ADMIN"))
            .build();

        AkhqClaimProviderController.AkhqClaimResponseV2 actual = akhqClaimProviderController.generateClaimV2(request);
        // AdminRoles
        assertLinesMatch(
            List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"
            ),
            actual.getRoles()
        );
        // Admin Regexp
        assertLinesMatch(List.of(".*$"), actual.getTopicsFilterRegexp());
        assertLinesMatch(List.of(".*$"), actual.getConnectsFilterRegexp());
        assertLinesMatch(List.of(".*$"), actual.getConsumerGroupsFilterRegexp());
    }

    @Test
    void computeAllowedRegexListTestSuccessFilterStartWith() {
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
                .build()
        );
        List<String> actual = akhqClaimProviderController.computeAllowedRegexListForResourceType(inputAcls,
            AccessControlEntry.ResourceType.TOPIC);

        assertEquals(4, actual.size());
        assertLinesMatch(
            List.of(
                "^\\Qproject1.\\E.*$",
                "^\\Qproject2.topic2\\E$",
                "^\\Qproject2.topic3\\E$",
                "^\\Qproject3.\\E.*$"
            ),
            actual
        );
    }
}
