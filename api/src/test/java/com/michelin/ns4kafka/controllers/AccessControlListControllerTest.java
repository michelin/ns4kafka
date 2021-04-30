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
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
public class AccessControlListControllerTest {
    @Mock
    AccessControlEntryService accessControlEntryService;
    @Mock
    NamespaceService namespaceService;

    @InjectMocks
    AccessControlListController accessControlListController;

    @Test
    void list() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        // granted by admin to test
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("admin").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        // granted by admin to test
        AccessControlEntry ace2 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("admin").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        // granted by test to namespace-other
        AccessControlEntry ace3 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("test").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("prefix")
                        .grantedTo("namespace-other")
                        .build()
                )
                .build();
        // granted by admin to namespace-other
        AccessControlEntry ace4 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("admin").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("other-prefix")
                        .grantedTo("namespace-other")
                        .build()
                )
                .build();
        // granted by namespace-other to test
        AccessControlEntry ace5 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("namespace-other").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("other-prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of(ace1, ace2, ace5));
        Mockito.when(accessControlEntryService.findAllForCluster("local"))
                .thenReturn(List.of(ace1, ace2, ace3, ace4, ace5));

        List<AccessControlEntry> actual = accessControlListController.list("test", Optional.of(AccessControlListController.AclLimit.GRANTEE));
        Assertions.assertEquals(3, actual.size());
        Assertions.assertTrue(actual.contains(ace1));
        Assertions.assertTrue(actual.contains(ace2));
        Assertions.assertTrue(actual.contains(ace5));

        actual = accessControlListController.list("test", Optional.of(AccessControlListController.AclLimit.GRANTOR));
        Assertions.assertEquals(1, actual.size());
        Assertions.assertTrue(actual.contains(ace3));

        actual = accessControlListController.list("test", Optional.of(AccessControlListController.AclLimit.ALL));
        Assertions.assertEquals(4, actual.size());
        Assertions.assertTrue(actual.contains(ace1));
        Assertions.assertTrue(actual.contains(ace2));
        Assertions.assertTrue(actual.contains(ace3));
        Assertions.assertTrue(actual.contains(ace5));

    }

    @Test
    void applyAsAdmin_Failure() {
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("admin").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Mockito.when(accessControlEntryService.validateAsAdmin(ace1))
                .thenReturn(List.of("ValidationError"));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> accessControlListController.apply("admin", ace1, false));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
    }
    @Test
    void applyAsAdmin_Success() {
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("admin").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Mockito.when(accessControlEntryService.validateAsAdmin(ace1))
                .thenReturn(List.of());
        Mockito.when(accessControlEntryService.create(ace1))
                .thenReturn(ace1);

        AccessControlEntry actual = accessControlListController.apply("admin", ace1, false);
        Assertions.assertEquals("admin", actual.getMetadata().getNamespace());
    }

    @Test
    void applyValidationErrors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("test").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of("ValidationError"));

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> accessControlListController.apply("test", ace1, false));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
    }

    @Test
    void applySuccess() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Mockito.when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        Mockito.when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of());
        Mockito.when(accessControlEntryService.create(ace1))
                .thenReturn(ace1);

        AccessControlEntry actual = accessControlListController.apply("test", ace1, false);
        Assertions.assertEquals("test", actual.getMetadata().getNamespace());
        Assertions.assertEquals("local", actual.getMetadata().getCluster());
    }

    @Test
    void deleteFailNotFound() {
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").namespace("test").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("prefix")
                        .grantedTo("namespace-other")
                        .build()
                )
                .build();
        Mockito.when(accessControlEntryService.findByName("test", "ace1"))
                .thenReturn(Optional.empty());
        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> accessControlListController.delete("test", "ace1"));

        Assertions.assertLinesMatch(List.of("Invalid value ace1 for name : AccessControlEntry doesn't exist in this namespace"), actual.getValidationErrors());
    }

    @Test
    void deleteSuccess() {
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").namespace("test").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("prefix")
                        .grantedTo("namespace-other")
                        .build()
                )
                .build();
        Mockito.when(accessControlEntryService.findByName("test", "ace1"))
                .thenReturn(Optional.of(ace1));
        //Mockito.doNothing().when(accessControlEntryService.delete(ace1));
        accessControlListController.delete("test", "ace1");
    }
}
