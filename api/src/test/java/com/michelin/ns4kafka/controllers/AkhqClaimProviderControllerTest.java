package com.michelin.ns4kafka.controllers;

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
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
public class AkhqClaimProviderControllerTest {
    @Mock
    NamespaceService namespaceService;
    @Mock
    AccessControlEntryService accessControlEntryService;

    @InjectMocks
    AkhqClaimProviderController akhqClaimProviderController;

    @Test
    void computeAllowedRegexList_TestEmpty(){
        List<AccessControlEntry> inputACLs = List.of(
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
        List<String> actual = akhqClaimProviderController.computeAllowedRegexListForResourceType(inputACLs, AccessControlEntry.ResourceType.CONNECT);

        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals("^none$", actual.get(0));
    }
    @Test
    void computeAllowedRegexList_TestSuccess(){
        List<AccessControlEntry> inputACLs = List.of(
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
        List<String> actual = akhqClaimProviderController.computeAllowedRegexListForResourceType(inputACLs, AccessControlEntry.ResourceType.TOPIC);

        Assertions.assertEquals(2, actual.size());
        Assertions.assertLinesMatch(
                List.of(
                        "^\\Qproject1.\\E.*$",
                        "^\\Qproject2.topic1\\E$"
                ),
                actual
        );
        Assertions.assertFalse(actual.contains("^\\Qproject1.connects\\E.*$"));
    }
    @Test
    void computeAllowedRegexList_TestSuccessDistinct(){
        List<AccessControlEntry> inputACLs = List.of(
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
        List<String> actual = akhqClaimProviderController.computeAllowedRegexListForResourceType(inputACLs, AccessControlEntry.ResourceType.TOPIC);

        Assertions.assertEquals(1, actual.size());
        Assertions.assertLinesMatch(
                List.of(
                        "^\\Qproject1.\\E.*$"
                ),
                actual
        );
    }

    @Test
    void generateClaim_TestNullOrEmptyRequest(){
        //null request
        AkhqClaimProviderController.AKHQClaimResponse actual = akhqClaimProviderController.generateClaim(null);

        Assertions.assertEquals(1, actual.getAttributes().get("topicsFilterRegexp").size());
        Assertions.assertEquals("^none$", actual.getAttributes().get("topicsFilterRegexp").get(0));

        //null group list
        AkhqClaimProviderController.AKHQClaimRequest request = AkhqClaimProviderController.AKHQClaimRequest.builder().build();
        actual = akhqClaimProviderController.generateClaim(request);

        Assertions.assertEquals(1, actual.getAttributes().get("topicsFilterRegexp").size());
        Assertions.assertEquals("^none$", actual.getAttributes().get("topicsFilterRegexp").get(0));

        //empty group list
        request = AkhqClaimProviderController.AKHQClaimRequest.builder().groups(List.of()).build();
        actual = akhqClaimProviderController.generateClaim(request);

        Assertions.assertEquals(1, actual.getAttributes().get("topicsFilterRegexp").size());
        Assertions.assertEquals("^none$", actual.getAttributes().get("topicsFilterRegexp").get(0));
    }

    @Test
    void generateClaim_TestSuccess(){
        Namespace ns1 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1")
                        .labels(Map.of("support-group","GP-PROJECT1-SUPPORT"))
                        .build())
                .build();
        Namespace ns2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns2")
                        .labels(Map.of("support-group","GP-PROJECT1-SUPPORT"))
                        .build())
                .build();
        Namespace ns3 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns3")
                        .labels(Map.of("support-group","GP-PROJECT2-SUPPORT"))
                        .build())
                .build();
        Namespace ns4 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns4")
                        .labels(Map.of("other-key","anything"))
                        .build())
                .build();
        Namespace ns5 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns5")
                        .build())
                .build();

        AccessControlEntry ns1_ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.")
                        .build())
                .build();
        AccessControlEntry ns1_ace2 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_c.")
                        .build())
                .build();
        AccessControlEntry ns2_ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project2_t.")
                        .build())
                .build();
        AccessControlEntry ns2_ace2 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("project1_t.") // ACL granted by ns1 to ns2
                        .build())
                .build();
        AccessControlEntry ns3_ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resource("project3_topic")
                        .build())
                .build();
        Mockito.when(namespaceService.listAll())
                .thenReturn(List.of(ns1, ns2, ns3, ns4, ns5));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns1))
                .thenReturn(List.of(ns1_ace1, ns1_ace2));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns2))
                .thenReturn(List.of(ns2_ace1, ns2_ace2));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns3))
                .thenReturn(List.of(ns3_ace1));

        AkhqClaimProviderController.AKHQClaimRequest request = AkhqClaimProviderController.AKHQClaimRequest.builder()
                .groups(List.of("GP-PROJECT1-SUPPORT", "GP-PROJECT2-SUPPORT"))
                .build();

        AkhqClaimProviderController.AKHQClaimResponse actual = akhqClaimProviderController.generateClaim(request);

        Mockito.verify(accessControlEntryService,Mockito.times(1)).findAllGrantedToNamespace(ns1);
        Mockito.verify(accessControlEntryService,Mockito.times(1)).findAllGrantedToNamespace(ns2);
        Mockito.verify(accessControlEntryService,Mockito.times(1)).findAllGrantedToNamespace(ns3);
        Mockito.verify(accessControlEntryService,Mockito.never()).findAllGrantedToNamespace(ns4);
        Mockito.verify(accessControlEntryService,Mockito.never()).findAllGrantedToNamespace(ns5);
        Assertions.assertLinesMatch(
                List.of(
                        "topic/read",
                        "topic/data/read",
                        "group/read",
                        "registry/read",
                        "connect/read"
                ),
                actual.getRoles()
        );
        Assertions.assertEquals(3, actual.getAttributes().get("topicsFilterRegexp").size());
        Assertions.assertLinesMatch(
                List.of(
                        "^\\Qproject1_t.\\E.*$",
                        "^\\Qproject2_t.\\E.*$",
                        "^\\Qproject3_topic\\E$"
                ),
                actual.getAttributes().get("topicsFilterRegexp")
        );

    }
}
