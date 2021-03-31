package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.web.router.RouteMatch;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Singleton
public class ResourceBasedSecurityRule implements SecurityRule {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceBasedSecurityRule.class);

    public static final String IS_ADMIN = "isAdmin()";

    private final Pattern namespacedResourcePattern = Pattern.compile("^\\/api\\/namespaces\\/(?<namespace>[a-zA-Z0-9_-]+)\\/(?<resourceType>[a-z]+)(\\/([a-zA-Z0-9_-]+)(\\/(?<resourceSubtype>[a-z]+))?)?");

    @Setter
    private String adminGroup;

    RoleBindingRepository roleBindingRepository;
    NamespaceRepository namespaceRepository;


    public ResourceBasedSecurityRule(@Value("${ns4kafka.admin.group:_}") String adminGroup, RoleBindingRepository roleBindingRepository, NamespaceRepository namespaceRepository) {
        this.adminGroup = adminGroup;
        this.roleBindingRepository = roleBindingRepository;
        this.namespaceRepository = namespaceRepository;
    }

    @Override
    public SecurityRuleResult check(HttpRequest<?> request, @Nullable RouteMatch<?> routeMatch, @Nullable Map<String, Object> claims) {
        //Unauthenticated request
        if (claims == null || !claims.keySet().containsAll( List.of("groups", "sub", "roles"))) {
            LOG.debug("No Authentication available for path [" + request.getPath() + "]. Returning unknown.");
            return SecurityRuleResult.UNKNOWN;
        }

        String sub = claims.get("sub").toString();
        List<String> groups = (List<String>) claims.get("groups");
        List<String> roles = (List<String>) claims.get("roles");

        //Request to a URL that is not in the scope of this SecurityRule
        Matcher matcher = namespacedResourcePattern.matcher(request.getPath());
        if (!matcher.find()) {
            LOG.debug("Invalid Namespaced Resource for path [" + request.getPath() + "]. Returning unknown.");
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
        //Admin specific namespace
        if(namespace.equals(Namespace.ADMIN_NAMESPACE) && roles.contains(IS_ADMIN)){
            LOG.debug("Authorized admin user [" + sub + "] on path [" + request.getPath() + "]. Returning ALLOWED.");
            return SecurityRuleResult.ALLOWED;
        }
        //Namespace doesn't exist
        if (namespaceRepository.findByName(namespace).isEmpty()) {
            LOG.debug("Namespace not found for user [" + sub + "] on path [" + request.getPath() + "]. Returning unknown.");
            return SecurityRuleResult.UNKNOWN;
        }
        //Admin are allowed everything (provided that the namespace exists)
        if(roles.contains(IS_ADMIN)){
            LOG.debug("Authorized admin user [" + sub + "] on path [" + request.getPath() + "]. Returning ALLOWED.");
            return SecurityRuleResult.ALLOWED;
        }

        //Collect all roleBindings for this user
        Collection<RoleBinding> roleBindings = roleBindingRepository.findAllForGroups(groups);
        List<RoleBinding> authorizedRoleBindings = roleBindings.stream()
                .filter(roleBinding -> roleBinding.getNamespace().equals(namespace))
                .filter(roleBinding -> roleBinding.getRole().getResourceTypes().contains(resourceType))
                .filter(roleBinding -> roleBinding.getRole().getVerbs().contains(request.getMethodName()))
                .collect(Collectors.toList());

        //User not authorized to access requested resource
        if (authorizedRoleBindings.isEmpty()) {
            LOG.debug("No matching RoleBinding for user [" + sub + "] on path [" + request.getPath() + "]. Returning unknown.");
            return SecurityRuleResult.UNKNOWN;
        }

        if (LOG.isDebugEnabled()) {
            authorizedRoleBindings.forEach(roleBinding -> LOG.debug("Found matching RoleBinding : " + roleBinding.toString()));
            LOG.debug("Authorized user [" + sub + "] on path [" + request.getPath() + "]");
        }


        return SecurityRuleResult.ALLOWED;
    }

    @Override
    public int getOrder() {
        return -1000;
    }

    public List<String> computeRolesFromGroups(List<String> groups) {
        List<String> roles = new ArrayList<>();
        if (groups.contains(adminGroup))
            roles.add(ResourceBasedSecurityRule.IS_ADMIN);
        //TODO other specific API groups ? auditor ?
        return roles;
    }
}
