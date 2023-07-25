package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.acl.AclController;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AclControllerTest {
    @Mock
    AccessControlEntryService accessControlEntryService;

    @Mock
    NamespaceService namespaceService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    AclController accessControlListController;

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
        // granted by admin to all (public)
        AccessControlEntry ace6 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("admin").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("public-prefix")
                        .grantedTo("*")
                        .build()
                )
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.findAllGrantedToNamespace(ns))
                .thenReturn(List.of(ace1, ace2, ace5, ace6));
        when(accessControlEntryService.findAllForCluster("local"))
                .thenReturn(List.of(ace1, ace2, ace3, ace4, ace5, ace6));

        List<AccessControlEntry> actual = accessControlListController.list("test", Optional.of(AclController.AclLimit.GRANTEE));
        assertEquals(4, actual.size());
        assertTrue(actual.contains(ace1));
        assertTrue(actual.contains(ace2));
        assertTrue(actual.contains(ace5));
        assertTrue(actual.contains(ace6));

        actual = accessControlListController.list("test", Optional.of(AclController.AclLimit.GRANTOR));
        assertEquals(1, actual.size());
        assertTrue(actual.contains(ace3));

        actual = accessControlListController.list("test", Optional.of(AclController.AclLimit.ALL));
        assertEquals(5, actual.size());
        assertTrue(actual.contains(ace1));
        assertTrue(actual.contains(ace2));
        assertTrue(actual.contains(ace3));
        assertTrue(actual.contains(ace5));
        assertTrue(actual.contains(ace6));

    }

    @Test
    void getAcl(){
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        // granted by tes to test
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").namespace("test").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        // granted by test to test
        AccessControlEntry ace2 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace2").namespace("test").cluster("local").build())
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
                .metadata(ObjectMeta.builder().name("ace3").namespace("test").cluster("local").build())
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
                .metadata(ObjectMeta.builder().name("ace4").namespace("admin").cluster("local").build())
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
                .metadata(ObjectMeta.builder().name("ace5").namespace("namespace-other").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("other-prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.findAllForCluster("local"))
                .thenReturn(List.of(ace1, ace2, ace3, ace4, ace5));

        // name not in list
        Optional<AccessControlEntry> result1 = accessControlListController.get("test", "ace6");
        // not granted to or assigned by me
        Optional<AccessControlEntry> result2 = accessControlListController.get("test", "ace4");
        // assigned by me
        Optional<AccessControlEntry> result3 = accessControlListController.get("test", "ace3");
        // granted to me
        Optional<AccessControlEntry> result4 = accessControlListController.get("test", "ace5");

        assertTrue(result1.isEmpty());
        assertTrue(result2.isEmpty());

        assertTrue(result3.isPresent());
        assertEquals(ace3, result3.get());

        assertTrue(result4.isPresent());
        assertEquals(ace5, result4.get());

    }

    @Test
    void applyAsAdmin_Failure() {
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
        Authentication auth = Authentication.build("admin", Map.of("roles",List.of("isAdmin()")));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validateAsAdmin(ace1, ns))
                .thenReturn(List.of("ValidationError"));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
                () -> accessControlListController.apply(auth,"test", ace1, false));
        assertEquals(1, actual.getValidationErrors().size());
    }

    @Test
    void applyAsAdmin_Success() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("acl-test").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Authentication auth = Authentication.build("admin", Map.of("roles",List.of("isAdmin()")));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validateAsAdmin(ace1, ns))
                .thenReturn(List.of());
        when(accessControlEntryService.create(ace1))
                .thenReturn(ace1);

        var response = accessControlListController.apply(auth,"test", ace1, false);
        AccessControlEntry actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
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
        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of("ValidationError"));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
                () -> accessControlListController.apply(auth,"test", ace1, false));
        assertEquals(1, actual.getValidationErrors().size());
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
        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(accessControlEntryService.create(ace1))
                .thenReturn(ace1);

        var response = accessControlListController.apply(auth, "test", ace1, false);
        AccessControlEntry actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
    }
    @Test
    void applySuccess_AlreadyExists() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of());
        when(accessControlEntryService.findByName("test","ace1"))
                .thenReturn(Optional.of(ace1));

        var response = accessControlListController.apply(auth, "test", ace1, false);
        AccessControlEntry actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
        verify(accessControlEntryService,never()).create(ArgumentMatchers.any());
    }

    @Test
    void applyFailed_ChangedSpec() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        AccessControlEntry ace1Old = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.CONNECT) //This line was changed
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of());
        when(accessControlEntryService.findByName("test","ace1"))
                .thenReturn(Optional.of(ace1Old));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
                () -> accessControlListController.apply(auth,"test", ace1, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertEquals("Invalid modification: `spec` is immutable. You can still update `metadata`", actual.getValidationErrors().get(0));

    }

    @Test
    void applySuccess_ChangedMetadata() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("ace1")
                        .labels(Map.of("new-label", "label-value")) // This label is new
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        AccessControlEntry ace1Old = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of());
        when(accessControlEntryService.findByName("test","ace1"))
                .thenReturn(Optional.of(ace1Old));
        when(accessControlEntryService.create(ace1))
                .thenReturn(ace1);

        var response = accessControlListController.apply(auth, "test", ace1, false);
        AccessControlEntry actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
        Assertions.assertFalse(actual.getMetadata().getLabels().isEmpty());

    }

    @Test
    void applySuccess_ChangedMetadataDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .name("ace1")
                        .labels(Map.of("new-label", "label-value")) // This label is new
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        AccessControlEntry ace1Old = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .resource("prefix")
                        .grantedTo("test")
                        .build()
                )
                .build();
        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of());
        when(accessControlEntryService.findByName("test","ace1"))
                .thenReturn(Optional.of(ace1Old));

        var response = accessControlListController.apply(auth, "test", ace1, true);
        AccessControlEntry actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
        Assertions.assertFalse(actual.getMetadata().getLabels().isEmpty());

    }

    @Test
    void applyDryRunAdmin() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();
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
        Authentication auth = Authentication.build("admin", Map.of("roles",List.of("isAdmin()")));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validateAsAdmin(ace1, ns))
                .thenReturn(List.of());

        var response = accessControlListController.apply(auth, "test", ace1, true);
        AccessControlEntry actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(accessControlEntryService, never()).create(ArgumentMatchers.any());
    }

    @Test
    void applyDryRun() {
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
                        .build())
                .build();

        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.validate(ace1, ns))
                .thenReturn(List.of());

        var response = accessControlListController.apply(auth, "test", ace1, true);
        AccessControlEntry actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(accessControlEntryService, never()).create(ace1);
    }

    /**
     * Validate ACL deletion fail when not found
     */
    @Test
    void deleteFailNotFound() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();

        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.findByName("test", "ace1"))
                .thenReturn(Optional.empty());

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
                () -> accessControlListController.delete(auth,"test", "ace1", false));

        assertLinesMatch(List.of("Invalid value ace1 for name: ACL does not exist in this namespace."), actual.getValidationErrors());
    }

    /**
     * Validate ACL deletion failed for self assigned
     */
    @Test
    void deleteFailSelfAssigned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();

        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").namespace("test").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("prefix")
                        .grantedTo("test")
                        .build())
                .build();

        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.findByName("test", "ace1"))
                .thenReturn(Optional.of(ace1));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
                () -> accessControlListController.delete(auth,"test", "ace1", false));

        assertLinesMatch(
                List.of("Only admins.*"),
                actual.getValidationErrors());
    }

    /**
     * Validate ACL deletion as admin
     */
    @Test
    void deleteSuccessSelfAssigned_AsAdmin() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();

        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").namespace("test").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("prefix")
                        .grantedTo("test")
                        .build())
                .build();

        Authentication auth = Authentication.build("user", Map.of("roles",List.of("isAdmin()")));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.findByName("test", "ace1"))
                .thenReturn(Optional.of(ace1));

        HttpResponse<Void> actual = accessControlListController.delete(auth,"test", "ace1", false);

        assertEquals(HttpStatus.NO_CONTENT, actual.status());
    }

    /**
     * Validate ACL deletion
     */
    @Test
    void deleteSuccess() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();

        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("ace1").namespace("test").cluster("local").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.READ)
                        .resource("prefix")
                        .grantedTo("namespace-other")
                        .build())
                .build();
        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.findByName("test", "ace1"))
                .thenReturn(Optional.of(ace1));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> actual = accessControlListController.delete(auth,"test", "ace1", false);

        assertEquals(HttpStatus.NO_CONTENT, actual.status());
    }

    /**
     * Validate ACL deletion in dry mode
     */
    @Test
    void deleteDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder().name("test").cluster("local").build())
                .build();

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
        Authentication auth = Authentication.build("user", Map.of("roles",List.of()));

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(accessControlEntryService.findByName("test", "ace1"))
                .thenReturn(Optional.of(ace1));
        HttpResponse<Void> actual = accessControlListController.delete(auth,"test", "ace1", true);

        verify(accessControlEntryService, never()).delete(any(), any());
        assertEquals(HttpStatus.NO_CONTENT, actual.status());
    }
}