package com.michelin.ns4kafka.security.auth.ldap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.security.auth.AuthenticationRoleBinding;
import com.michelin.ns4kafka.security.auth.AuthenticationService;
import io.micronaut.security.authentication.AuthenticationResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Ldap authentication mapper test.
 */
@ExtendWith(MockitoExtension.class)
class LdapAuthenticationMapperTest {
    @Mock
    AuthenticationService authenticationService;

    @InjectMocks
    LdapAuthenticationMapper ldapAuthenticationMapper;

    @Test
    void shouldMapAttributesToAuthenticationResponse() {
        AuthenticationRoleBinding authenticationRoleBinding = AuthenticationRoleBinding.builder()
            .namespace("namespace")
            .verbs(List.of(RoleBinding.Verb.GET))
            .resourceTypes(List.of("topics"))
            .build();

        AuthenticationResponse authenticationResponse = AuthenticationResponse.success("username", null,
            Map.of("roleBindings", List.of(authenticationRoleBinding)));

        when(authenticationService.buildAuthJwtGroups("username", List.of("group-1")))
            .thenReturn(authenticationResponse);

        AuthenticationResponse response =
            ldapAuthenticationMapper.map(null, "username", Set.of("group-1"));

        assertTrue(response.isAuthenticated());
        assertTrue(response.getAuthentication().isPresent());
        assertEquals("username", response.getAuthentication().get().getName());
        assertIterableEquals(List.of(authenticationRoleBinding),
            (List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes().get("roleBindings"));
    }
}
