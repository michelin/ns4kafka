package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
public class AccessControlEntryServiceTest {
    @Mock
    AccessControlEntryRepository accessControlEntryRepository;
    @Mock
    NamespaceService namespaceService;

    @InjectMocks
    AccessControlEntryService accessControlEntryService;

    @Test
    void validate_NotAllowedResources() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        AccessControlEntry badACL = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .namespace("namespace")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("test")
                        .grantedTo("target-ns")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("target-ns"))
                .thenReturn(Optional.empty());
        Mockito.when(accessControlEntryRepository.findAll())
                .thenReturn(List.of());
        List<String> actual = accessControlEntryService.validate(badACL, ns);
        Assertions.assertLinesMatch(List.of(
                "^Invalid value CONNECT for resourceType.*",
                "^Invalid value OWNER for permission.*",
                "^Invalid value target-ns for grantedTo.*",
                "^Invalid grant PREFIXED:.*"),
                actual);

    }

    @Test
    void validate_NotAllowedSelfGrant() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        AccessControlEntry badACL = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .namespace("namespace")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("test")
                        .grantedTo("namespace")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));
        Mockito.when(accessControlEntryRepository.findAll())
                .thenReturn(List.of());
        List<String> actual = accessControlEntryService.validate(badACL, ns);
        Assertions.assertLinesMatch(List.of(
                "^Invalid value namespace for grantedTo.*",
                "^Invalid grant PREFIXED:.*"),
                actual);

    }

    @Test
    void validate_NotAllowedOwnerOfBadPrefix() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .namespace("namespace")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("main")
                        .grantedTo("target-ns")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("target-ns"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(accessControlEntryRepository.findAll())
                .thenReturn(List.of(AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .permission(AccessControlEntry.Permission.OWNER)
                                .resource("main.sub")
                                .grantedTo("namespace")
                                .build()
                        )
                        .build()
                ));
        List<String> actual = accessControlEntryService.validate(accessControlEntry, ns);
        Assertions.assertLinesMatch(List.of("^Invalid grant PREFIXED:.*"), actual);
    }

    @Test
    void validate_NotAllowedOwnerOfBadLiteral() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .namespace("namespace")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("resource2")
                        .grantedTo("target-ns")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("target-ns"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(accessControlEntryRepository.findAll())
                .thenReturn(List.of(AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .permission(AccessControlEntry.Permission.OWNER)
                                .resource("resource1")
                                .grantedTo("namespace")
                                .build()
                        )
                        .build()
                ));
        List<String> actual = accessControlEntryService.validate(accessControlEntry, ns);
        Assertions.assertLinesMatch(List.of("^Invalid grant LITERAL:.*"), actual);
    }

    @Test
    void validate_AllowedOwnerOfLiteral() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .namespace("namespace")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("resource1")
                        .grantedTo("target-ns")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("target-ns"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(accessControlEntryRepository.findAll())
                .thenReturn(List.of(AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .permission(AccessControlEntry.Permission.OWNER)
                                .resource("resource1")
                                .grantedTo("namespace")
                                .build()
                        )
                        .build()
                ));
        List<String> actual = accessControlEntryService.validate(accessControlEntry, ns);
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void validate_AllowedOwnerOfPrefix() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .namespace("namespace")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("main.sub")
                        .grantedTo("target-ns")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("target-ns"))
                .thenReturn(Optional.of(Namespace.builder().build()));
        Mockito.when(accessControlEntryRepository.findAll())
                .thenReturn(List.of(AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .permission(AccessControlEntry.Permission.OWNER)
                                .resource("main")
                                .grantedTo("namespace")
                                .build()
                        )
                        .build()
                ));
        List<String> actual = accessControlEntryService.validate(accessControlEntry, ns);
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void validateAsAdmin_MissingNamespaceAndCluster() {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .namespace("admin")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("main.sub")
                        .grantedTo("target-ns")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("target-ns"))
                .thenReturn(Optional.empty());
        List<String> actual = accessControlEntryService.validateAsAdmin(accessControlEntry);

        Assertions.assertEquals(2, actual.size());
        Assertions.assertIterableEquals(List.of(
                "Invalid value null for cluster: Value must be non-null",
                "Invalid value target-ns for grantedTo: Namespace doesn't exist"),
                actual);
    }
    @Test
    void validateAsAdmin_InvalidCluster() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("target-ns")
                        .cluster("local")
                        .build())
                .build();
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .cluster("local-wrong")
                        .namespace("admin")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("main.sub")
                        .grantedTo("target-ns")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("target-ns"))
                .thenReturn(Optional.of(ns));
        List<String> actual = accessControlEntryService.validateAsAdmin(accessControlEntry);

        Assertions.assertEquals(1, actual.size());
        Assertions.assertIterableEquals(List.of(
                "Invalid value local-wrong for cluster: Value must be the same as the Namespace [local]"),
                actual);
    }
    @Test
    void validateAsAdmin_Success() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("target-ns")
                        .cluster("local")
                        .build())
                .build();
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("acl-name")
                        .cluster("local")
                        .namespace("admin")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("main.sub")
                        .grantedTo("target-ns")
                        .build())
                .build();
        Mockito.when(namespaceService.findByName("target-ns"))
                .thenReturn(Optional.of(ns));
        List<String> actual = accessControlEntryService.validateAsAdmin(accessControlEntry);

        Assertions.assertTrue(actual.isEmpty());
    }
    @Test
    void findAllGrantedToNamespace() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("namespace1").build()).build();
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder().grantedTo("namespace1").build()).build();
        AccessControlEntry ace2 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder().grantedTo("namespace1").build()).build();
        AccessControlEntry ace3 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder().grantedTo("namespace2").build()).build();

        Mockito.when(accessControlEntryRepository.findAll())
                .thenReturn(List.of(ace1, ace2, ace3));
        List<AccessControlEntry> actual = accessControlEntryService.findAllGrantedToNamespace(ns);
        Assertions.assertEquals(2, actual.size());
    }

    @Test
    void isNamespaceOwnerOfResource() {
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("main")
                        .grantedTo("namespace")
                        .build()
                )
                .build();
        AccessControlEntry ace2 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("connect")
                        .grantedTo("namespace")
                        .build()
                )
                .build();
        AccessControlEntry ace3 = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .permission(AccessControlEntry.Permission.WRITE)
                        .resource("connect")
                        .grantedTo("namespace-other")
                        .build()
                )
                .build();
        Mockito.when(accessControlEntryRepository.findAll())
                .thenReturn(List.of(ace1, ace2, ace3));
        Assertions.assertTrue(
                accessControlEntryService.isNamespaceOwnerOfResource("namespace",
                        AccessControlEntry.ResourceType.CONNECT,
                        "connect"));
        Assertions.assertTrue(
                accessControlEntryService.isNamespaceOwnerOfResource("namespace",
                        AccessControlEntry.ResourceType.TOPIC,
                        "main"));
        Assertions.assertTrue(
                accessControlEntryService.isNamespaceOwnerOfResource("namespace",
                        AccessControlEntry.ResourceType.TOPIC,
                        "main.sub"), "subresource");
        Assertions.assertFalse(
                accessControlEntryService.isNamespaceOwnerOfResource("namespace-other",
                        AccessControlEntry.ResourceType.TOPIC,
                        "main"));
        Assertions.assertFalse(
                accessControlEntryService.isNamespaceOwnerOfResource("namespace-other",
                        AccessControlEntry.ResourceType.CONNECT,
                        "connect"));
    }
}
