package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controller.acl.AclController;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AclControllerTest {
    @Mock
    AclService aclService;

    @Mock
    NamespaceService namespaceService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Mock
    SecurityService securityService;

    @InjectMocks
    AclController accessControlListController;

    @Test
    void shouldListAcls() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedOwnerAdminToTest = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .namespace("admin")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        AccessControlEntry aceConnectPrefixedOwnerAdminToTest = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .namespace("admin")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedReadTestToNamespaceOther = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("prefix")
                .grantedTo("namespace-other")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedOwnerAdminToNamespaceOther = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .namespace("admin")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("other-prefix")
                .grantedTo("namespace-other")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedReadNamespaceOtherToTest = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .namespace("namespace-other")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("other-prefix")
                .grantedTo("test")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedReadAdminToAll = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .namespace("admin")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("public-prefix")
                .grantedTo("*")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.findAllGrantedToNamespace(namespace)).thenReturn(
            List.of(aceTopicPrefixedOwnerAdminToTest, aceConnectPrefixedOwnerAdminToTest,
                aceTopicPrefixedReadNamespaceOtherToTest, aceTopicPrefixedReadAdminToAll));
        when(aclService.findAllForCluster("local")).thenReturn(
            List.of(aceTopicPrefixedOwnerAdminToTest, aceConnectPrefixedOwnerAdminToTest,
                aceTopicPrefixedReadTestToNamespaceOther, aceTopicPrefixedOwnerAdminToNamespaceOther,
                aceTopicPrefixedReadNamespaceOtherToTest, aceTopicPrefixedReadAdminToAll));

        List<AccessControlEntry> actual = accessControlListController
            .list("test", Optional.of(AclController.AclLimit.GRANTEE));

        assertEquals(4, actual.size());
        assertTrue(actual.contains(aceTopicPrefixedOwnerAdminToTest));
        assertTrue(actual.contains(aceConnectPrefixedOwnerAdminToTest));
        assertTrue(actual.contains(aceTopicPrefixedReadNamespaceOtherToTest));
        assertTrue(actual.contains(aceTopicPrefixedReadAdminToAll));

        actual = accessControlListController.list("test", Optional.of(AclController.AclLimit.GRANTOR));
        assertEquals(1, actual.size());
        assertTrue(actual.contains(aceTopicPrefixedReadTestToNamespaceOther));

        actual = accessControlListController.list("test", Optional.of(AclController.AclLimit.ALL));
        assertEquals(5, actual.size());
        assertTrue(actual.contains(aceTopicPrefixedOwnerAdminToTest));
        assertTrue(actual.contains(aceConnectPrefixedOwnerAdminToTest));
        assertTrue(actual.contains(aceTopicPrefixedReadTestToNamespaceOther));
        assertTrue(actual.contains(aceTopicPrefixedReadNamespaceOtherToTest));
        assertTrue(actual.contains(aceTopicPrefixedReadAdminToAll));
    }

    @Test
    void shouldGetAcl() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedOwnerTestToTest = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        AccessControlEntry aceConnectPrefixedOwnerTestToTest = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace2")
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedReadTestToNamespaceOther = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace3")
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("prefix")
                .grantedTo("namespace-other")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedOwnerAdminToNamespaceOther = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace4")
                .namespace("admin")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("other-prefix")
                .grantedTo("namespace-other")
                .build())
            .build();

        AccessControlEntry aceTopicPrefixedReadNamespaceOtherToTest = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace5")
                .namespace("namespace-other")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("other-prefix")
                .grantedTo("test")
                .build())
            .build();

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.findAllForCluster("local")).thenReturn(
            List.of(aceTopicPrefixedOwnerTestToTest, aceConnectPrefixedOwnerTestToTest,
                aceTopicPrefixedReadTestToNamespaceOther, aceTopicPrefixedOwnerAdminToNamespaceOther,
                aceTopicPrefixedReadNamespaceOtherToTest));

        // Name not in list
        Optional<AccessControlEntry> result1 = accessControlListController.get("test", "ace6");
        assertTrue(result1.isEmpty());

        // Not granted to or assigned by me
        Optional<AccessControlEntry> result2 = accessControlListController.get("test", "ace4");

        assertTrue(result2.isEmpty());

        // Assigned by me
        Optional<AccessControlEntry> result3 = accessControlListController.get("test", "ace3");

        assertTrue(result3.isPresent());
        assertEquals(aceTopicPrefixedReadTestToNamespaceOther, result3.get());

        // Granted to me
        Optional<AccessControlEntry> result4 = accessControlListController.get("test", "ace5");

        assertTrue(result4.isPresent());
        assertEquals(aceTopicPrefixedReadNamespaceOtherToTest, result4.get());
    }

    @Test
    void shouldApplyFailsAsAdmin() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        Authentication authentication = Authentication.build("admin", List.of("isAdmin()"),
            Map.of(ROLES, List.of("isAdmin()")));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validateAsAdmin(accessControlEntry, namespace))
            .thenReturn(List.of("ValidationError"));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> accessControlListController.apply(authentication, "test", accessControlEntry, false));
        assertEquals(1, actual.getValidationErrors().size());
    }

    @Test
    void shouldApplyWithSuccessAsAdmin() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("acl-test")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        Authentication authentication = Authentication.build("admin", List.of("isAdmin()"),
            Map.of(ROLES, List.of("isAdmin()")));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validateAsAdmin(accessControlEntry, namespace)).thenReturn(List.of());
        when(aclService.create(accessControlEntry)).thenReturn(accessControlEntry);

        var response = accessControlListController.apply(authentication, "test", accessControlEntry, false);
        AccessControlEntry actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
    }

    @Test
    void shouldApplyFailWithValidationErrors() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validate(accessControlEntry, namespace)).thenReturn(List.of("ValidationError"));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> accessControlListController.apply(authentication, "test", accessControlEntry, false));
        assertEquals(1, actual.getValidationErrors().size());
    }

    @Test
    void shouldApplyAclWithSuccess() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validate(accessControlEntry, namespace)).thenReturn(List.of());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());
        when(aclService.create(accessControlEntry)).thenReturn(accessControlEntry);

        var response = accessControlListController.apply(authentication, "test", accessControlEntry, false);
        AccessControlEntry actual = response.body();
        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
    }

    @Test
    void shouldEndApplyWithSuccessWhenAclAlreadyExists() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validate(accessControlEntry, namespace)).thenReturn(List.of());
        when(aclService.findByName("test", "ace1")).thenReturn(Optional.of(accessControlEntry));

        var response = accessControlListController.apply(authentication, "test", accessControlEntry, false);
        AccessControlEntry actual = response.body();
        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
        verify(aclService, never()).create(ArgumentMatchers.any());
    }

    @Test
    void shouldApplyFailWhenSpecChanges() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        AccessControlEntry oldAccessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.CONNECT) // This line was changed
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validate(accessControlEntry, namespace)).thenReturn(List.of());
        when(aclService.findByName("test", "ace1")).thenReturn(Optional.of(oldAccessControlEntry));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> accessControlListController.apply(authentication, "test", accessControlEntry, false));
        assertEquals(1, actual.getValidationErrors().size());
        assertEquals("Invalid \"apply\" operation: field \"spec\" is immutable.",
            actual.getValidationErrors().get(0));
    }

    @Test
    void shouldApplyAclWithSuccessWhenMetadataChanges() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .labels(Map.of("new-label", "label-value")) // This label is new
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        AccessControlEntry oldAccessControlEntry =
            AccessControlEntry.builder()
                .metadata(Metadata.builder()
                    .name("ace1")
                    .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .permission(AccessControlEntry.Permission.OWNER)
                    .resource("prefix")
                    .grantedTo("test")
                    .build())
                .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validate(accessControlEntry, namespace)).thenReturn(List.of());
        when(aclService.findByName("test", "ace1")).thenReturn(Optional.of(oldAccessControlEntry));
        when(aclService.create(accessControlEntry)).thenReturn(accessControlEntry);

        var response = accessControlListController.apply(authentication, "test", accessControlEntry, false);
        AccessControlEntry actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
        assertFalse(actual.getMetadata().getLabels().isEmpty());
    }

    @Test
    void shouldApplyAclWithSuccessWhenMetadataChangesInDryRunMode() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .labels(Map.of("new-label", "label-value")) // This label is new
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        AccessControlEntry oldAccessControlEntry =
            AccessControlEntry.builder()
                .metadata(Metadata.builder()
                    .name("ace1")
                    .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .permission(AccessControlEntry.Permission.OWNER)
                    .resource("prefix")
                    .grantedTo("test")
                    .build())
                .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(ns));
        when(aclService.validate(accessControlEntry, ns)).thenReturn(List.of());
        when(aclService.findByName("test", "ace1")).thenReturn(Optional.of(oldAccessControlEntry));

        var response = accessControlListController.apply(authentication, "test", accessControlEntry, true);
        AccessControlEntry actual = response.body();
        assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("test", actual.getMetadata().getNamespace());
        assertEquals("local", actual.getMetadata().getCluster());
        assertFalse(actual.getMetadata().getLabels().isEmpty());
    }

    @Test
    void shouldApplyAclInDryRunModeAsAdmin() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry =
            AccessControlEntry.builder()
                .metadata(Metadata.builder()
                    .namespace("admin")
                    .cluster("local")
                    .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                    .resourceType(AccessControlEntry.ResourceType.TOPIC)
                    .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                    .permission(AccessControlEntry.Permission.OWNER)
                    .resource("prefix")
                    .grantedTo("test")
                    .build())
                .build();

        Authentication authentication = Authentication.build("admin", List.of("isAdmin()"),
            Map.of(ROLES, List.of("isAdmin()")));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validateAsAdmin(accessControlEntry, namespace)).thenReturn(List.of());

        var response = accessControlListController.apply(authentication, "test", accessControlEntry, true);

        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(aclService, never()).create(ArgumentMatchers.any());
    }

    @Test
    void shouldApplyAclInDryRunMode() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("prefix")
                .grantedTo("test")
                .build())
            .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(namespaceService.findByName("test")).thenReturn(Optional.of(namespace));
        when(aclService.validate(accessControlEntry, namespace)).thenReturn(List.of());

        var response = accessControlListController.apply(authentication, "test", accessControlEntry, true);

        assertEquals("created", response.header("X-Ns4kafka-Result"));
        verify(aclService, never()).create(accessControlEntry);
    }

    @Test
    void shouldDeleteAclFailWhenNotFound() {
        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(aclService.findByName("test", "ace1")).thenReturn(Optional.empty());

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> accessControlListController.delete(authentication, "test", "ace1", false));

        assertEquals("Invalid value \"ace1\" for field \"name\": resource not found.",
            actual.getValidationErrors().get(0));
    }

    @Test
    void shouldDeleteSelfAssignedAclFailWhenNotAdmin() {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("prefix")
                .grantedTo("test").build())
            .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(aclService.findByName("test", "ace1")).thenReturn(Optional.of(accessControlEntry));

        ResourceValidationException actual = assertThrows(ResourceValidationException.class,
            () -> accessControlListController.delete(authentication, "test", "ace1", false));

        assertEquals("Invalid value \"ace1\" for field \"name\": only administrators can delete this ACL.",
            actual.getValidationErrors().get(0));
    }

    @Test
    void shouldDeleteSelfAssignedAclWithSuccessAsAdmin() {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("prefix")
                .grantedTo("test").build())
            .build();

        Authentication authentication = Authentication.build("user", List.of("isAdmin()"),
            Map.of("roles", List.of("isAdmin()")));

        when(aclService.findByName("test", "ace1")).thenReturn(Optional.of(accessControlEntry));

        HttpResponse<Void> actual = accessControlListController.delete(authentication, "test", "ace1", false);

        assertEquals(HttpStatus.NO_CONTENT, actual.status());
    }

    @Test
    void shouldDeleteAclWithSuccess() {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("prefix")
                .grantedTo("namespace-other")
                .build())
            .build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(aclService.findByName("test", "ace1")).thenReturn(Optional.of(accessControlEntry));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> actual = accessControlListController.delete(authentication, "test", "ace1", false);

        assertEquals(HttpStatus.NO_CONTENT, actual.status());
    }

    @Test
    void shouldDeleteInDryRunMode() {
        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ace1")
                .namespace("test")
                .cluster("local")
                .build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.READ)
                .resource("prefix")
                .grantedTo("namespace-other")
                .build()).build();

        Authentication authentication = Authentication.build("user", Map.of("roles", List.of()));

        when(aclService.findByName("test", "ace1")).thenReturn(Optional.of(accessControlEntry));
        HttpResponse<Void> actual = accessControlListController.delete(authentication, "test", "ace1", true);

        verify(aclService, never()).delete(any());
        assertEquals(HttpStatus.NO_CONTENT, actual.status());
    }
}
