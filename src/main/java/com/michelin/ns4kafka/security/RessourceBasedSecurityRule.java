package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.models.role.RoleBinding;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStore;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.web.router.RouteMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Singleton
public class RessourceBasedSecurityRule implements SecurityRule {
    private static final Logger LOG = LoggerFactory.getLogger(RessourceBasedSecurityRule.class);
    @Inject
    RoleBindingRepository roleBindingRepository;

    @Override
    public SecurityRuleResult check(HttpRequest<?> request, @Nullable RouteMatch<?> routeMatch, @Nullable Map<String, Object> claims) {
        if(routeMatch != null && claims != null && claims.containsKey("roles")){
            List<String> roles = (List<String>)claims.get("roles");
            Map<String, Object> vals = routeMatch.getVariableValues();
            if(vals.containsKey("namespace") && vals.containsKey("resourceType")){
                String resourcePath = "/api/namespace/"+vals.get("namespace");
                Collection<RoleBinding> roleBindings = roleBindingRepository.findAllForGroups(roles);
                boolean authorized = roleBindings.stream()
                        .map(roleBinding -> "/api/namespace/"+roleBinding.getNamespace())
                        .anyMatch(s -> s.equals(resourcePath));
                if(authorized){
                    LOG.debug("Accepted requested resourcePath: "+resourcePath);
                    return SecurityRuleResult.ALLOWED;
                }
                // TODO Full RBAC with <resourceName> and <verb>
                // /api/namespaces/<namespace>/<resourceType>/<resourceName>/<verb>
                if(roles.contains(resourcePath)){
                    return SecurityRuleResult.ALLOWED;
                }
            }
            // TODO remove bypass
            if (roles.contains("f4m"))
                return SecurityRuleResult.ALLOWED;
        }
        return SecurityRuleResult.UNKNOWN;
    }

    @Override
    public int getOrder() {
        return -1000;
    }
}
