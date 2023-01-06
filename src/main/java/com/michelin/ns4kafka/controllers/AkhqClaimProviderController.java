package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.config.AkhqClaimProviderControllerConfig;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.NamespaceService;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.rules.SecurityRule;
import lombok.Builder;
import lombok.Getter;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@RolesAllowed(SecurityRule.IS_ANONYMOUS)
@Controller("/akhq-claim")
public class AkhqClaimProviderController {
    private static final List<String> EMPTY_REGEXP = List.of("^none$");
    private static final List<String> ADMIN_REGEXP = List.of(".*");
    @Inject
    AkhqClaimProviderControllerConfig config;
    @Inject
    AccessControlEntryService accessControlEntryService;
    @Inject
    NamespaceService namespaceService;

    // For AKHQ up to 0.19
    @Post
    public AKHQClaimResponse generateClaim(@Valid @Body AKHQClaimRequest request) {
        if (request == null) {
            return AKHQClaimResponse.ofEmpty(config.getRoles());
        }

        final List<String> groups = Optional.ofNullable(request.getGroups()).orElse(new ArrayList<>());

        if (groups.contains(config.getAdminGroup())) {
            return AKHQClaimResponse.ofAdmin(config.getAdminRoles());
        }

        List<AccessControlEntry> relatedACL = namespaceService.listAll()
                .stream()
                // keep namespaces with correct label
                .filter(namespace -> namespace.getMetadata().getLabels() != null &&
                        groups.contains(namespace.getMetadata().getLabels().getOrDefault(config.getGroupLabel(), "_"))
                )
                // find all ACL associated to these namespaces
                .flatMap(namespace -> accessControlEntryService.findAllGrantedToNamespace(namespace).stream())
                .collect(Collectors.toList());

        // Add all public ACLs.
        relatedACL.addAll(accessControlEntryService.findAllPublicGrantedTo());

        return AKHQClaimResponse.builder()
                .roles(config.getRoles())
                .attributes(
                        Map.of(
                                "topicsFilterRegexp", computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.TOPIC),
                                "connectsFilterRegexp", computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.CONNECT),
                                "consumerGroupsFilterRegexp", ADMIN_REGEXP
                        )
                )
                .build();
    }
    // For AKHQ 0.20.0 and later
    @Post("/v2")
    public AKHQClaimResponseV2 generateClaimV2(@Valid @Body AKHQClaimRequest request) {
        if (request == null) {
            return AKHQClaimResponseV2.ofEmpty(config.getRoles());
        }

        final List<String> groups = Optional.ofNullable(request.getGroups()).orElse(new ArrayList<>());

        if (groups.contains(config.getAdminGroup())) {
            return AKHQClaimResponseV2.ofAdmin(config.getAdminRoles());
        }

        List<AccessControlEntry> relatedACL = namespaceService.listAll()
                .stream()
                // keep namespaces with correct label
                .filter(namespace -> namespace.getMetadata().getLabels() != null &&
                        groups.contains(namespace.getMetadata().getLabels().getOrDefault(config.getGroupLabel(), "_"))
                )
                // find all ACL associated to these namespaces
                .flatMap(namespace -> accessControlEntryService.findAllGrantedToNamespace(namespace).stream())
                .collect(Collectors.toList());

        // Add all public ACLs.
        relatedACL.addAll(accessControlEntryService.findAllPublicGrantedTo());

        return AKHQClaimResponseV2.builder()
                .roles(config.getRoles())
                .topicsFilterRegexp(computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.TOPIC))
                .connectsFilterRegexp(computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.CONNECT))
                .consumerGroupsFilterRegexp(ADMIN_REGEXP)
                .build();
    }

    public List<String> computeAllowedRegexListForResourceType(List<AccessControlEntry> acls, AccessControlEntry.ResourceType resourceType) {
        List<String> allowedRegex = acls.stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == resourceType)
                .map(accessControlEntry -> {
                    String escapedString = Pattern.quote(accessControlEntry.getSpec().getResource());
                    if (accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED) {
                        return String.format("^%s.*$", escapedString);
                    } else {
                        return String.format("^%s$", escapedString);
                    }
                })
                .distinct()
                .collect(Collectors.toList());
        //AKHQ considers empty list as "^.*$" so we must return something
        return !allowedRegex.isEmpty() ? allowedRegex : EMPTY_REGEXP;
    }

    @Introspected
    @Builder
    @Getter
    public static class AKHQClaimRequest {
        String providerType;
        String providerName;
        String username;
        List<String> groups;
    }

    @Introspected
    @Builder
    @Getter
    public static class AKHQClaimResponse {
        private List<String> roles;
        private Map<String, List<String>> attributes;

        public static AKHQClaimResponse ofEmpty(List<String> roles){
            return AKHQClaimResponse.builder()
                    .roles(roles)
                    .attributes(Map.of(
                            //AKHQ considers empty list as "^.*$" so we must return something
                            "topicsFilterRegexp", EMPTY_REGEXP,
                            "connectsFilterRegexp", EMPTY_REGEXP,
                            "consumerGroupsFilterRegexp", EMPTY_REGEXP
                    ))
                    .build();
        }
        public static AKHQClaimResponse ofAdmin(List<String> roles){

            return AKHQClaimResponse.builder()
                    .roles(roles)
                    .attributes(Map.of(
                            //AKHQ considers empty list as "^.*$" so we must return something
                            "topicsFilterRegexp", ADMIN_REGEXP,
                            "connectsFilterRegexp", ADMIN_REGEXP,
                            "consumerGroupsFilterRegexp", ADMIN_REGEXP
                    ))
                    .build();
        }
    }
    @Introspected
    @Builder
    @Getter
    public static class AKHQClaimResponseV2 {
        private List<String> roles;
        private List<String> topicsFilterRegexp;
        private List<String> connectsFilterRegexp;
        private List<String> consumerGroupsFilterRegexp;

        public static AKHQClaimResponseV2 ofEmpty(List<String> roles){
            return AKHQClaimResponseV2.builder()
                    .roles(roles)
                    .topicsFilterRegexp(EMPTY_REGEXP)
                    .connectsFilterRegexp(EMPTY_REGEXP)
                    .consumerGroupsFilterRegexp(EMPTY_REGEXP)
                    .build();
        }
        public static AKHQClaimResponseV2 ofAdmin(List<String> roles){

            return AKHQClaimResponseV2.builder()
                    .roles(roles)
                    .topicsFilterRegexp(ADMIN_REGEXP)
                    .connectsFilterRegexp(ADMIN_REGEXP)
                    .consumerGroupsFilterRegexp(ADMIN_REGEXP)
                    .build();
        }
    }
}
