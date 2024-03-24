package com.michelin.ns4kafka.security;

import static com.michelin.ns4kafka.security.auth.JwtField.JwtRoleBindingField.NAMESPACE;
import static com.michelin.ns4kafka.security.auth.JwtField.JwtRoleBindingField.RESOURCE_TYPES;
import static com.michelin.ns4kafka.security.auth.JwtField.JwtRoleBindingField.VERBS;
import static com.michelin.ns4kafka.security.auth.JwtField.ROLES;
import static com.michelin.ns4kafka.security.auth.JwtField.ROLE_BINDINGS;
import static com.michelin.ns4kafka.security.auth.JwtField.SUB;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.properties.SecurityProperties;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import com.michelin.ns4kafka.security.auth.JwtRoleBinding;
import com.michelin.ns4kafka.utils.exceptions.ForbiddenNamespaceException;
import com.michelin.ns4kafka.utils.exceptions.UnknownNamespaceException;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.security.rules.SecurityRuleResult;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

/**
 * Security rule to check if a user can access a given URL.
 */
@Slf4j
@Singleton
public class ResourceBasedSecurityRule implements SecurityRule<HttpRequest<?>> {
    public static final String IS_ADMIN = "isAdmin()";

    public static final String RESOURCE_PATTERN = "[a-zA-Z0-9_.-]";

    private final Pattern namespacedResourcePattern = Pattern.compile(
        "^/api/namespaces/(?<namespace>" + RESOURCE_PATTERN + "+)"
            + "/(?<resourceType>[a-z_-]+)(/(" + RESOURCE_PATTERN + "+)(/(?<resourceSubtype>[a-z-]+))?)?$");

    @Inject
    SecurityProperties securityProperties;

    @Inject
    RoleBindingRepository roleBindingRepository;

    @Inject
    NamespaceRepository namespaceRepository;

    @Override
    public Publisher<SecurityRuleResult> check(@Nullable HttpRequest<?> request,
                                               @Nullable Authentication authentication) {
        return Publishers.just(checkSecurity(request, authentication));
    }

    /**
     * Check a user can access a given URL.
     *
     * @param request        The current request
     * @param authentication The claims from the token
     * @return A security rule allowing the user or not
     */
    public SecurityRuleResult checkSecurity(HttpRequest<?> request, @Nullable Authentication authentication) {
        if (authentication == null) {
            return SecurityRuleResult.UNKNOWN;
        }

        if (!authentication.getAttributes().keySet().containsAll(List.of(ROLE_BINDINGS, SUB, ROLES))) {
            log.debug("No authentication available for path [{}]. Returning unknown.", request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        // Request to a URL that is not in the scope of this SecurityRule
        Matcher matcher = namespacedResourcePattern.matcher(request.getPath());
        if (!matcher.find()) {
            log.debug("Invalid namespaced resource for path [{}]. Returning unknown.", request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        String namespace = matcher.group("namespace");
        String resourceSubtype = matcher.group("resourceSubtype");
        String resourceType;

        // Subresource handling ie. connects/restart or groups/reset
        if (StringUtils.isNotEmpty(resourceSubtype)) {
            resourceType = matcher.group("resourceType") + "/" + resourceSubtype;
        } else {
            resourceType = matcher.group("resourceType");
        }

        // Namespace doesn't exist
        String sub = authentication.getName();
        if (namespaceRepository.findByName(namespace).isEmpty()) {
            log.debug("Namespace not found for user \"{}\" on path \"{}\"", sub, request.getPath());
            throw new UnknownNamespaceException(namespace);
        }

        // Admin are allowed everything (provided that the namespace exists)
        Collection<String> roles = authentication.getRoles();
        if (roles.contains(IS_ADMIN)) {
            log.debug("Authorized admin \"{}\" on path \"{}\"", sub, request.getPath());
            return SecurityRuleResult.ALLOWED;
        }

        // No role binding for the target namespace. User is targeting a namespace that he is not allowed to access
        List<JwtRoleBinding> namespaceRoleBindings =
            ((List<Map<String, ?>>) authentication.getAttributes().get(ROLE_BINDINGS))
                .stream()
                .map(roleBinding -> JwtRoleBinding.builder()
                    .namespace(roleBinding.get(NAMESPACE).toString())
                    .verbs((List<RoleBinding.Verb>) roleBinding.get(VERBS))
                    .resourceTypes((List<String>) roleBinding.get(RESOURCE_TYPES))
                    .build())
                .filter(roleBinding -> roleBinding.getNamespace().equals(namespace))
                .toList();

        if (namespaceRoleBindings.isEmpty()) {
            log.debug("No matching role binding for user \"{}\" and namespace \"{}\" on path \"{}\"", sub,
                namespace, request.getPath());
            throw new ForbiddenNamespaceException(namespace);
        }

        List<JwtRoleBinding> authorizedRoleBindings = namespaceRoleBindings
            .stream()
            .filter(roleBinding -> roleBinding.getResourceTypes().contains(resourceType))
            .filter(roleBinding -> roleBinding.getVerbs()
                .stream()
                .map(Enum::name)
                .toList()
                .contains(request.getMethodName()))
            .toList();

        // User not authorized to access requested resource
        if (authorizedRoleBindings.isEmpty()) {
            log.debug("No matching role binding for user \"{}\", namespace \"{}\", resource type \"{}\" "
                    + "and HTTP verb \"{}\" on path \"{}\"",
                sub, namespace, resourceType, request.getMethodName(), request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        if (log.isDebugEnabled()) {
            authorizedRoleBindings.forEach(
                roleBinding -> log.debug("Found matching role binding \"{}\"", roleBinding.toString()));
            log.debug("Authorized user \"{}\" on path \"{}\"", sub, request.getPath());
        }

        return SecurityRuleResult.ALLOWED;
    }

    @Override
    public int getOrder() {
        return -1000;
    }

    /**
     * Compute roles from groups.
     *
     * @param groups The groups
     * @return The roles
     */
    public List<String> computeRolesFromGroups(List<String> groups) {
        List<String> roles = new ArrayList<>();

        if (groups.contains(securityProperties.getAdminGroup())) {
            roles.add(ResourceBasedSecurityRule.IS_ADMIN);
        }

        return roles;
    }
}
