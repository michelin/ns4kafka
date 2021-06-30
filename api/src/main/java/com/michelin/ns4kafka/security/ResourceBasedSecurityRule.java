package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.web.router.RouteMatch;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class ResourceBasedSecurityRule implements SecurityRule {

    public static final String IS_ADMIN = "isAdmin()";

    private final Pattern namespacedResourcePattern = Pattern.compile("^\\/api\\/namespaces\\/(?<namespace>[a-zA-Z0-9_-]+)\\/(?<resourceType>[a-z_-]+)(\\/([a-zA-Z0-9_-]+)(\\/(?<resourceSubtype>[a-z]+))?)?");


    SecurityConfig securityConfig;
    RoleBindingRepository roleBindingRepository;
    NamespaceRepository namespaceRepository;


    public ResourceBasedSecurityRule(SecurityConfig securityConfig, RoleBindingRepository roleBindingRepository, NamespaceRepository namespaceRepository) {
        this.securityConfig = securityConfig;
        this.roleBindingRepository = roleBindingRepository;
        this.namespaceRepository = namespaceRepository;
    }

    @Override
    public SecurityRuleResult check(HttpRequest<?> request, @Nullable RouteMatch<?> routeMatch, @Nullable Map<String, Object> claims) {
        //Unauthenticated request
        if (claims == null || !claims.keySet().containsAll( List.of("groups", "sub", "roles"))) {
            log.debug("No Authentication available for path [{}]. Returning UNKNOWN.", request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        String sub = claims.get("sub").toString();
        List<String> groups = (List<String>) claims.get("groups");
        List<String> roles = (List<String>) claims.get("roles");

        //Request to a URL that is not in the scope of this SecurityRule
        Matcher matcher = namespacedResourcePattern.matcher(request.getPath());
        if (!matcher.find()) {
            log.debug("Invalid Namespaced Resource for path [{}]. Returning UNKNOWN.",request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        String namespace = matcher.group("namespace");
        String resourceSubtype = matcher.group("resourceSubtype");
        String resourceType;
        //Subresource handling ie. connects/restart or groups/reset
        if (StringUtils.isNotEmpty(resourceSubtype)) {
            resourceType = matcher.group("resourceType") + "/" + resourceSubtype;
        } else {
            resourceType = matcher.group("resourceType");
        }

        //Namespace doesn't exist
        if (namespaceRepository.findByName(namespace).isEmpty()) {
            log.debug("Namespace not found for user [{}] on path [{}]. Returning UNKNOWN.", sub , request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }
        //Admin are allowed everything (provided that the namespace exists)
        if(roles.contains(IS_ADMIN)){
            log.debug("Authorized admin user [{}] on path [{}]. Returning ALLOWED.", sub, request.getPath());
            return SecurityRuleResult.ALLOWED;
        }

        //Collect all roleBindings for this user
        Collection<RoleBinding> roleBindings = roleBindingRepository.findAllForGroups(groups);
        List<RoleBinding> authorizedRoleBindings = roleBindings.stream()
                .filter(roleBinding -> roleBinding.getMetadata().getNamespace().equals(namespace))
                .filter(roleBinding -> roleBinding.getSpec().getRole().getResourceTypes().contains(resourceType))
                .filter(roleBinding -> roleBinding.getSpec().getRole().getVerbs().stream().map(v -> v.name()).collect(Collectors.toList()).contains(request.getMethodName()))
                .collect(Collectors.toList());

        //User not authorized to access requested resource
        if (authorizedRoleBindings.isEmpty()) {
            log.debug("No matching RoleBinding for user [{}] on path [{}]. Returning UNKNOWN.", sub, request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        if (log.isDebugEnabled()) {
            authorizedRoleBindings.forEach(roleBinding -> log.debug("Found matching RoleBinding : {}", roleBinding.toString()));
            log.debug("Authorized user [{}] on path [{}]", sub, request.getPath());
        }


        return SecurityRuleResult.ALLOWED;
    }

    @Override
    public int getOrder() {
        return -1000;
    }

    public List<String> computeRolesFromGroups(List<String> groups) {
        List<String> roles = new ArrayList<>();
        if (groups.contains(securityConfig.getAdminGroup()))
            roles.add(ResourceBasedSecurityRule.IS_ADMIN);
        //TODO other specific API groups ? auditor ?
        return roles;
    }
}
