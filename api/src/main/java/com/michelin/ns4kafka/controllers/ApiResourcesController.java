package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@RolesAllowed(SecurityRule.IS_ANONYMOUS)
@Controller("/api-resources")
public class ApiResourcesController {
    public static final ResourceDefinition ACL = ResourceDefinition.builder()
            .kind("AccessControlEntry")
            .namespaced(true)
            .synchronizable(false)
            .path("acls")
            .names(List.of("acls", "acl", "ac"))
            .build();
    public static final ResourceDefinition CONNECTOR = ResourceDefinition.builder()
            .kind("Connector")
            .namespaced(true)
            .synchronizable(true)
            .path("connects")
            .names(List.of("connects", "connect", "co"))
            .build();
    public static final ResourceDefinition KSTREAM = ResourceDefinition.builder()
            .kind("KafkaStream")
            .namespaced(true)
            .synchronizable(false)
            .path("streams")
            .names(List.of("streams", "stream", "st"))
            .build();
    public static final ResourceDefinition ROLE_BINDING = ResourceDefinition.builder()
            .kind("RoleBinding")
            .namespaced(true)
            .synchronizable(false)
            .path("role-bindings")
            .names(List.of("rolebindings", "rolebinding", "rb"))
            .build();
    public static final ResourceDefinition TOPIC = ResourceDefinition.builder()
            .kind("Topic")
            .namespaced(true)
            .synchronizable(true)
            .path("topics")
            .names(List.of("topics", "topic", "to"))
            .build();
    public static final ResourceDefinition SCHEMA = ResourceDefinition.builder()
            .kind("Schema")
            .namespaced(true)
            .synchronizable(false)
            .path("schemas")
            .names(List.of("schemas", "schemas", "sc"))
            .build();
    public static final ResourceDefinition NAMESPACE = ResourceDefinition.builder()
            .kind("Namespace")
                        .namespaced(false)
                        .synchronizable(false)
                        .path("namespaces")
                        .names(List.of("namespaces", "namespace", "ns"))
            .build();

    @Inject
    RoleBindingRepository roleBindingRepository;

    @Get
    public List<ResourceDefinition> list(@Nullable Authentication authentication) {
        List<ResourceDefinition> all = List.of(
                ACL,
                CONNECTOR,
                KSTREAM,
                ROLE_BINDING,
                TOPIC,
                NAMESPACE,
                SCHEMA
        );
        if(authentication==null){
            return all; // Backward compatibility for cli <= 1.3.0
        }
        List<String> roles = (List<String>)authentication.getAttributes().getOrDefault("roles", List.of());
        List<String> groups = (List<String>) authentication.getAttributes().getOrDefault("groups",List.of());

        if(roles.contains(ResourceBasedSecurityRule.IS_ADMIN)) {
            return all;
        }

        Collection<RoleBinding> roleBindings = roleBindingRepository.findAllForGroups(groups);
        List<String> authorizedResources = roleBindings.stream()
                .flatMap(roleBinding -> roleBinding.getSpec().getRole().getResourceTypes().stream())
                .distinct()
                .collect(Collectors.toList());
        return all.stream()
                .filter(resourceDefinition -> authorizedResources.contains(resourceDefinition.getPath()))
                .collect(Collectors.toList());
    }

    @Introspected
    @Builder
    @Getter
    @Setter
    public static class ResourceDefinition {
        private String kind;
        private boolean namespaced;
        private boolean synchronizable;
        private String path;
        private List<String> names;
    }
}
