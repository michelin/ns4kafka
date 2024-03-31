package com.michelin.ns4kafka.security.auth;

import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.security.authentication.Authentication;
import java.util.Collection;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

/**
 * Authentication info.
 * Type-safe representation of the authentication information and attributes.
 * This class can be injected in controllers to get the authenticated user information.
 *
 * @see <a href="https://micronaut-projects.github.io/micronaut-security/latest/guide/#customAuthenticatedUser">Micronaut Custom Binding</a>
 */
@Getter
@Builder
public class AuthenticationInfo {
    private String name;
    private Collection<String> roles;
    private Collection<AuthenticationRoleBinding> roleBindings;

    /**
     * Create an AuthenticationInfo from an Authentication.
     *
     * @param authentication the authentication
     * @return the authentication info
     */
    public static AuthenticationInfo of(Authentication authentication) {
        AuthenticationInfoBuilder builder = AuthenticationInfo.builder()
            .name(authentication.getName())
            .roles(authentication.getRoles());

        // Type-safe the role bindings attribute
        // JWT authentication role bindings attributes is a List of Map<String, String>
        // Basic authentication role bindings attributes is already a List<AuthenticationRoleBinding>
        // In all cases, convert attributes to avoid generic type or unchecked cast warnings
        List<?> roleBindings = (List<?>) authentication.getAttributes().get(ROLE_BINDINGS);
        ObjectMapper objectMapper = new ObjectMapper();
        List<AuthenticationRoleBinding> typedRoleBindings = roleBindings
            .stream()
            .map(roleBinding -> objectMapper.convertValue(roleBinding, AuthenticationRoleBinding.class))
            .toList();

        return builder
            .roleBindings(typedRoleBindings)
            .build();
    }
}
