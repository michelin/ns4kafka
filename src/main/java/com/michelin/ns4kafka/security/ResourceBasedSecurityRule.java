package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.web.router.RouteMatch;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
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

    @Getter
    @Value("${ns4kafka.admin.group:_}")
    private String adminGroup;

    @Inject
    RoleBindingRepository roleBindingRepository;

    @Inject
    NamespaceRepository namespaceRepository;

    Pattern namespacedResourcePattern = Pattern.compile("^\\/api\\/namespaces\\/(?<namespace>[a-zA-Z0-9_-]+)\\/(?<resourceType>[a-z]+)(\\/([a-zA-Z0-9_-]+)(\\/(?<resourceSubtype>[a-z]+))?)?");

    @Override
    public SecurityRuleResult check(HttpRequest<?> request, @Nullable RouteMatch<?> routeMatch, @Nullable Map<String, Object> claims) {
        //If the request corresponds to a Controller entry
        if(routeMatch != null && claims != null && claims.containsKey("groups") && claims.containsKey("sub")){
            String sub = claims.get("sub").toString();
            List<String> groups = (List<String>)claims.get("groups");

            LOG.info("API call from "+sub+ " on resource "+routeMatch.toString());
            // Not using routeMatch to get the resourceType and resourceSubtype values
            Matcher matcher = namespacedResourcePattern.matcher(request.getPath());
            while (matcher.find()){
                //namespaced resource handling
                Collection<RoleBinding> roleBindings = roleBindingRepository.findAllForGroups(groups);
                //TODO users + groups
                // roleBindings.addAll(roleBindingRepository.findAllForUser(request.getUserPrincipal().get().getName()))

                // 1. Namespace must exist ?
                // 2. Namespace resourceType must be allowed by RoleBinding
                // 3. Request VERB must be allowed by RoleBinding

                LOG.debug("Checking user " + sub + " against request "+request.getPath());

                String namespace = matcher.group("namespace");
                String resourceType = matcher.group("resourceType");
                String resourceSubtype = matcher.group("resourceSubtype");
                String finalResource = resourceType + (resourceSubtype!=null? "/" + resourceSubtype : "");
                List<RoleBinding> authorizedRoleBindings = roleBindings.stream()
                        .filter(roleBinding -> roleBinding.getNamespace().equals(namespace))
                        .filter(roleBinding -> roleBinding.getRole().getResourceTypes().contains(finalResource))
                        .filter(roleBinding -> roleBinding.getRole().getVerbs().contains(request.getMethodName()))
                        .collect(Collectors.toList());
                if(authorizedRoleBindings.size()>0) {
                    if(LOG.isDebugEnabled()){
                        authorizedRoleBindings.forEach(roleBinding -> LOG.debug("Found matching RoleBinding : "+roleBinding.toString()));
                    }
                    //TODO is this the good place for this (ns exists check) ?
                    if(namespaceRepository.findByName(namespace).isPresent()) {
                        LOG.debug("Authorized user "+sub+" : Matching RoleBinding");
                        return SecurityRuleResult.ALLOWED;
                    }else{
                        LOG.info("Denied user " + sub + " : Namespace doesn't exist");
                    }
                }else{
                    LOG.info("Denied user "+ sub + " : No matching RoleBinding");
                }
            }
            //TODO Non-namespaced resource handling
            // /admin ?
        }
        return SecurityRuleResult.UNKNOWN;
    }

    @Override
    public int getOrder() {
        return -1000;
    }

    public List<String> computeRolesFromGroups(List<String> groups) {
        List<String> roles = new ArrayList<>();
        if(groups.contains(adminGroup))
            roles.add(ResourceBasedSecurityRule.IS_ADMIN);
        //TODO other specific API groups ? auditor ?
        return roles;
    }
}
