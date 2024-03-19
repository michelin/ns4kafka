package com.michelin.ns4kafka.security.auth;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.properties.SecurityProperties;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.RoleBindingService;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Authentication service.
 */
@Slf4j
@Singleton
public class AuthenticationService {
    @Inject
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Inject
    RoleBindingService roleBindingService;

    @Inject
    SecurityProperties securityProperties;

    /**
     * Build an authentication response with the user details.
     *
     * @param username The username
     * @param groups   The user groups
     * @return An authentication response with the user details
     */
    public AuthenticationResponse buildAuthJwtGroups(String username, List<String> groups) {
        List<RoleBinding> roleBindings = roleBindingService.listByGroups(groups);
        if (roleBindings.isEmpty() && !groups.contains(securityProperties.getAdminGroup())) {
            log.debug("Error during authentication: user groups not found in any namespace");
            throw new AuthenticationException(new AuthenticationFailed("No namespace matches your groups"));
        }

        return AuthenticationResponse.success(username, resourceBasedSecurityRule.computeRolesFromGroups(groups),
            Map.of("role-bindings", roleBindings
                .stream()
                .map(roleBinding -> JwtRoleBinding.builder()
                    .namespace(roleBinding.getMetadata().getNamespace())
                    .verbs(new ArrayList<>(roleBinding.getSpec().getRole().getVerbs()))
                    .resources(new ArrayList<>(roleBinding.getSpec().getRole().getResourceTypes()))
                    .build())
                .toList()));
    }
}
