package com.michelin.ns4kafka.controllers;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.rules.SecurityRule;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.security.RolesAllowed;
import java.util.List;

@RolesAllowed(SecurityRule.IS_ANONYMOUS)
@Controller("/api-resources")
public class ApiResourcesController {

    @Get
    public List<ResourceDefinition> list() {
        return List.of(
                // Namespaced Resources
                ResourceDefinition.builder()
                        .kind("AccessControlEntry")
                        .namespaced(true)
                        .path("acls")
                        .names(List.of("acls", "acl", "ac"))
                        .build(),
                ResourceDefinition.builder()
                        .kind("Connector")
                        .namespaced(true)
                        .path("connects")
                        .names(List.of("connects", "connect", "co"))
                        .build(),
                ResourceDefinition.builder()
                        .kind("RoleBinding")
                        .namespaced(true)
                        .path("role-bindings")
                        .names(List.of("rolebindings", "rolebinding", "rb"))
                        .build(),
                ResourceDefinition.builder()
                        .kind("Topic")
                        .namespaced(true)
                        .path("topics")
                        .names(List.of("topics", "topic", "to"))
                        .build(),
                // Non-Namespaced Resources
                ResourceDefinition.builder()
                        .kind("Namespace")
                        .namespaced(false)
                        .path("namespaces")
                        .names(List.of("namespaces", "namespace", "ns"))
                        .build()
                );
    }

    @Introspected
    @Builder
    @Getter
    @Setter
    public static class ResourceDefinition {
        private String kind;
        private boolean namespaced;
        private String path;
        private List<String> names;
    }
}
