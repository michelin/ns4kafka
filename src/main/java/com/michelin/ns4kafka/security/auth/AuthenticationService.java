package com.michelin.ns4kafka.security.auth;

import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.property.SecurityProperties;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.RoleBindingService;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
        List<RoleBinding> roleBindings = roleBindingService.findAllByGroups(groups);
        if (roleBindings.isEmpty() && !groups.contains(securityProperties.getAdminGroup())) {
            log.debug("Error during authentication: user groups not found in any namespace");
            throw new AuthenticationException(new AuthenticationFailed("No namespace matches your groups"));
        }

        return AuthenticationResponse.success(username, resourceBasedSecurityRule.computeRolesFromGroups(groups),
            Map.of(ROLE_BINDINGS, roleBindings
                .stream()
                // group the namespaces by verbs + resourceTypes in a mapping
                .collect(Collectors.groupingBy(roleBinding ->
                    new AuthenticationRoleBinding.VerbResourceTypes(
                        new ArrayList<>(roleBinding.getSpec().getRole().getVerbs()),
                        new ArrayList<>(roleBinding.getSpec().getRole().getResourceTypes())
                    ),
                    Collectors.mapping(rb -> rb.getMetadata().getNamespace(), Collectors.toList())
                ))
                // build JWT with a list of namespaces for each combination of verbs + resourceTypes
                .entrySet()
                .stream()
                .map(entry -> AuthenticationRoleBinding.builder()
                    .namespaces(entry.getValue())
                    .verbs(entry.getKey().verbs())
                    .resourceTypes(entry.getKey().resourceTypes())
                    .build())
                .toList()));
    }
}
