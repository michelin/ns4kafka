package com.michelin.ns4kafka.controllers;

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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@RolesAllowed(SecurityRule.IS_ANONYMOUS)
@Controller("/akhq-claim")
public class AkhqClaimProviderController {
    public static String AKHQ_SUPPORT_LABEL_KEY = "support-group";
    public static List<String> AKHQ_EMPTY_REGEX_LIST = List.of("^none$");

    @Inject
    AccessControlEntryService accessControlEntryService;
    @Inject
    NamespaceService namespaceService;

    List<String> akhqRoles = List.of(
            "topic/read",
            "topic/data/read",
            "group/read",
            "registry/read",
            "connect/read"
    );

    @Post
    public AKHQClaimResponse generateClaim(@Valid @Body AKHQClaimRequest request) {
        if (request == null || request.getGroups() == null || request.getGroups().isEmpty()) {
            return AKHQClaimResponse.builder()
                    .roles(akhqRoles)
                    .attributes(
                            Map.of(
                                    //AKHQ considers empty list as "^.*$" so we must return something
                                    "topicsFilterRegexp", AKHQ_EMPTY_REGEX_LIST,
                                    "connectsFilterRegexp", AKHQ_EMPTY_REGEX_LIST,
                                    "consumerGroupsFilterRegexp", AKHQ_EMPTY_REGEX_LIST
                            )
                    )
                    .build();
        }
        List<AccessControlEntry> relatedACL = namespaceService.listAll()
                .stream()
                // keep namespaces with correct label
                .filter(namespace -> namespace.getMetadata().getLabels() != null &&
                        request.getGroups().contains(namespace.getMetadata().getLabels().getOrDefault(AKHQ_SUPPORT_LABEL_KEY, "_"))
                )
                // find all ACL associated to these namespaces
                .flatMap(namespace -> accessControlEntryService.findAllGrantedToNamespace(namespace).stream())
                .collect(Collectors.toList());

        return AKHQClaimResponse.builder()
                .roles(akhqRoles)
                .attributes(
                        Map.of(
                                "topicsFilterRegexp", computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.TOPIC),
                                "connectsFilterRegexp", computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.CONNECT),
                                "consumerGroupsFilterRegexp", computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.GROUP)
                        )
                )
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
        return !allowedRegex.isEmpty() ? allowedRegex : AKHQ_EMPTY_REGEX_LIST;
    }

    @Introspected
    @Builder
    @Getter
    static class AKHQClaimRequest {
        String providerType;
        String providerName;
        String username;
        List<String> groups;
    }

    @Introspected
    @Builder
    @Getter
    static class AKHQClaimResponse {
        private List<String> roles;
        private Map<String, List<String>> attributes;
    }
}
