package com.michelin.ns4kafka.security.auth;

import static com.michelin.ns4kafka.model.RoleBinding.Verb.GET;
import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.micronaut.security.authentication.Authentication;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Authentication info test.
 */
@ExtendWith(MockitoExtension.class)
public class AuthenticationInfoTest {

    @Test
    void shouldConvertFromMapRoleBindingsType() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ROLE_BINDINGS, List.of(Map.of("namespace", "namespace",
            "verbs", List.of("GET"),
            "resourceTypes", List.of("topics"))));
        Authentication authentication = Authentication.build("name", List.of("role"), attributes);

        AuthenticationInfo authenticationInfo = AuthenticationInfo.of(authentication);

        assertEquals("name", authenticationInfo.getName());
        assertEquals(List.of("role"), authenticationInfo.getRoles().stream().toList());
        assertIterableEquals(List.of(new AuthenticationRoleBinding("namespace", List.of(GET), List.of("topics"))),
            authenticationInfo.getRoleBindings().stream().toList());
    }

    @Test
    void shouldConvertFromAuthenticationRoleBindingsType() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ROLE_BINDINGS, List.of(new AuthenticationRoleBinding("namespace", List.of(GET),
            List.of("topics"))));
        Authentication authentication = Authentication.build("name", List.of("role"), attributes);

        AuthenticationInfo authenticationInfo = AuthenticationInfo.of(authentication);

        assertEquals("name", authenticationInfo.getName());
        assertEquals(List.of("role"), authenticationInfo.getRoles().stream().toList());
        assertIterableEquals(List.of(new AuthenticationRoleBinding("namespace", List.of(GET), List.of("topics"))),
            authenticationInfo.getRoleBindings().stream().toList());
    }
}
