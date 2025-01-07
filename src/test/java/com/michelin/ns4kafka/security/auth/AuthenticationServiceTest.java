package com.michelin.ns4kafka.security.auth;

import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.property.SecurityProperties;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.RoleBindingService;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationResponse;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthenticationServiceTest {
    @Mock
    RoleBindingService roleBindingService;

    @Mock
    SecurityProperties securityProperties;

    @Mock
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @InjectMocks
    AuthenticationService authenticationService;

    @Test
    void shouldThrowErrorWhenNoRoleBindingAndNotAdmin() {
        when(roleBindingService.findAllByGroups(any()))
            .thenReturn(Collections.emptyList());

        when(securityProperties.getAdminGroup())
            .thenReturn("admin");

        List<String> groups = List.of("group");
        AuthenticationException exception = assertThrows(AuthenticationException.class,
            () -> authenticationService.buildAuthJwtGroups("username", groups));

        assertTrue(exception.getResponse().getMessage().isPresent());
        assertEquals("No namespace matches your groups", exception.getResponse().getMessage().get());
    }

    @Test
    void shouldReturnAuthenticationSuccessWhenAdminNoGroup() {
        when(roleBindingService.findAllByGroups(any()))
            .thenReturn(Collections.emptyList());

        when(securityProperties.getAdminGroup())
            .thenReturn("admin");

        when(resourceBasedSecurityRule.computeRolesFromGroups(any()))
            .thenReturn(List.of(ResourceBasedSecurityRule.IS_ADMIN));

        AuthenticationResponse response = authenticationService.buildAuthJwtGroups("admin", List.of("admin"));

        assertTrue(response.getAuthentication().isPresent());
        assertEquals("admin", response.getAuthentication().get().getName());
        assertTrue(response.getAuthentication().get().getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN));
        assertTrue(response.getAuthentication().get().getAttributes()
            .containsKey(ROLE_BINDINGS));
        assertTrue(
            ((List<?>) response.getAuthentication().get().getAttributes().get(ROLE_BINDINGS)).isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnAuthenticationSuccessWhenAdminWithGroups() {
        RoleBinding roleBinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb")
                .namespace("ns1")
                .build())
            .spec(RoleBinding.RoleBindingSpec.builder()
                .role(RoleBinding.Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                    .build())
                .subject(RoleBinding.Subject.builder()
                    .subjectName("group1")
                    .subjectType(RoleBinding.SubjectType.GROUP)
                    .build())
                .build())
            .build();

        when(roleBindingService.findAllByGroups(any()))
            .thenReturn(List.of(roleBinding));

        when(resourceBasedSecurityRule.computeRolesFromGroups(any()))
            .thenReturn(List.of(ResourceBasedSecurityRule.IS_ADMIN));

        AuthenticationResponse response = authenticationService.buildAuthJwtGroups("admin", List.of("admin"));

        assertTrue(response.getAuthentication().isPresent());
        assertEquals("admin", response.getAuthentication().get().getName());
        assertTrue(response.getAuthentication().get().getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN));
        assertTrue(response.getAuthentication().get().getAttributes()
            .containsKey("roleBindings"));
        assertEquals(List.of("ns1"),
            ((List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                .get("roleBindings")).getFirst()
                .getNamespaces());
        assertTrue(
            ((List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                .get("roleBindings")).getFirst()
                .getVerbs()
                .containsAll(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET)));
        assertTrue(
            ((List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                .get("roleBindings")).getFirst()
                .getResourceTypes()
                .containsAll(List.of("topics", "acls")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnAuthenticationSuccessWhenUserWithGroups() {
        RoleBinding roleBinding = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb")
                .namespace("ns1")
                .build())
            .spec(RoleBinding.RoleBindingSpec.builder()
                .role(RoleBinding.Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                    .build())
                .subject(RoleBinding.Subject.builder()
                    .subjectName("group1")
                    .subjectType(RoleBinding.SubjectType.GROUP)
                    .build())
                .build())
            .build();

        when(roleBindingService.findAllByGroups(any()))
            .thenReturn(List.of(roleBinding));

        when(resourceBasedSecurityRule.computeRolesFromGroups(any()))
            .thenReturn(List.of());

        AuthenticationResponse response = authenticationService.buildAuthJwtGroups("user", List.of("group"));

        assertTrue(response.getAuthentication().isPresent());
        assertEquals("user", response.getAuthentication().get().getName());
        assertTrue(response.getAuthentication().get().getRoles().isEmpty());
        assertTrue(response.getAuthentication().get().getAttributes()
            .containsKey("roleBindings"));
        assertEquals(List.of("ns1"),
            ((List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                .get("roleBindings")).getFirst()
                .getNamespaces());
        assertTrue(
            ((List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                .get("roleBindings")).getFirst()
                .getVerbs()
                .containsAll(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET)));
        assertTrue(
            ((List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                .get("roleBindings")).getFirst()
                .getResourceTypes()
                .containsAll(List.of("topics", "acls")));
    }
}
