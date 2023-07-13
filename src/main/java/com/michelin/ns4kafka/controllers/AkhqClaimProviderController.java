package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.config.AkhqClaimProviderControllerConfig;
import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.NamespaceService;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import javax.annotation.security.RolesAllowed;
import javax.validation.Valid;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Tag(name = "AKHQ", description = "Manage the AKHQ endpoints.")
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

    @Inject
    List<KafkaAsyncExecutorConfig> managedClusters;

    /**
     * List AKHQ claims (v019 and prior)
     * @param request The AKHQ request
     * @return The AKHQ claims
     */
    @Post
    public AKHQClaimResponse generateClaim(@Valid @Body AKHQClaimRequest request) {
        if (request == null) {
            return AKHQClaimResponse.ofEmpty(config.getFormerRoles());
        }

        final List<String> groups = Optional.ofNullable(request.getGroups()).orElse(new ArrayList<>());

        if (groups.contains(config.getAdminGroup())) {
            return AKHQClaimResponse.ofAdmin(config.getFormerAdminRoles());
        }

        List<AccessControlEntry> relatedACL = namespaceService.listAll()
                .stream()
                .filter(namespace -> namespace.getMetadata().getLabels() != null &&
                        groups.contains(namespace.getMetadata().getLabels().getOrDefault(config.getGroupLabel(), "_")))
                .flatMap(namespace -> accessControlEntryService.findAllGrantedToNamespace(namespace).stream())
                .collect(Collectors.toList());

        // Add all public ACLs.
        relatedACL.addAll(accessControlEntryService.findAllPublicGrantedTo());

        return AKHQClaimResponse.builder()
                .roles(config.getFormerRoles())
                .attributes(
                        Map.of(
                                "topicsFilterRegexp", computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.TOPIC),
                                "connectsFilterRegexp", computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.CONNECT),
                                "consumerGroupsFilterRegexp", ADMIN_REGEXP
                        )
                )
                .build();
    }

    /**
     * List AKHQ claims (v020 to 024)
     * @param request The AKHQ request
     * @return The AKHQ claims
     */
    @Post("/v2")
    public AKHQClaimResponseV2 generateClaimV2(@Valid @Body AKHQClaimRequest request) {
        if (request == null) {
            return AKHQClaimResponseV2.ofEmpty(config.getFormerRoles());
        }

        final List<String> groups = Optional.ofNullable(request.getGroups()).orElse(new ArrayList<>());

        if (groups.contains(config.getAdminGroup())) {
            return AKHQClaimResponseV2.ofAdmin(config.getFormerAdminRoles());
        }

        List<AccessControlEntry> relatedACL = getAllAclForGroups(groups);

        // Add all public ACLs.
        relatedACL.addAll(accessControlEntryService.findAllPublicGrantedTo());

        return AKHQClaimResponseV2.builder()
                .roles(config.getFormerRoles())
                .topicsFilterRegexp(computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.TOPIC))
                .connectsFilterRegexp(computeAllowedRegexListForResourceType(relatedACL, AccessControlEntry.ResourceType.CONNECT))
                .consumerGroupsFilterRegexp(ADMIN_REGEXP)
                .build();
    }

    /**
     * List AKHQ claims (v025 and higher)
     *
     * @param request The AKHQ request
     * @return The AKHQ claims
     */
    @Post("/v3")
    public AKHQClaimResponseV3 generateClaimV3(@Valid @Body AKHQClaimRequest request) {

        final List<String> groups = Optional.ofNullable(request.getGroups()).orElse(new ArrayList<>());

        if (groups.contains(config.getAdminGroup())) {
            return AKHQClaimResponseV3.ofAdmin(config.getAdminRoles());
        }

        List<AccessControlEntry> relatedACL = getAllAclForGroups(groups);

        // Add all public ACLs
        relatedACL.addAll(accessControlEntryService.findAllPublicGrantedTo());

        Map<String, AKHQClaimResponseV3.Group> bindings = new LinkedHashMap<>();

        // Start by creating a map that store permissions by role/cluster
        relatedACL.forEach(acl -> {
            String escapedString = Pattern.quote(acl.getSpec().getResource());
            String patternRegex;

            // Build the pattern regex based on the pattern type and the resource
            if (acl.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED) {
                patternRegex = String.format("^%s.*$", escapedString);
            } else {
                patternRegex = String.format("^%s$", escapedString);
            }
            // Build the cluster regex
            String patternCluster = String.format("^%s$", acl.getMetadata().getCluster());

            String role = config.getRoles().get(acl.getSpec().getResourceType());
            String key = role + "-" + acl.getSpec().getResource();

            // If we already have permissions for the role and cluster, add the pattern to the existing one
            if (bindings.containsKey(key)) {
                bindings.get(key).getClusters().add(patternCluster);
            } else {
                List<String> regexes = new ArrayList<>();
                regexes.add(patternRegex);
                List<String> clusters = new ArrayList<>();
                clusters.add(patternCluster);

                // Otherwise we add a new one
                bindings.put(key, AKHQClaimResponseV3.Group.builder()
                        .role(role)
                        .patterns(regexes)
                        .clusters(clusters)
                        .build());
            }
        });

        List<AKHQClaimResponseV3.Group> result = optimizeV3Claim(bindings);

        // Add the same pattern and cluster filtering for SCHEMA as the TOPIC ones
        result.addAll(result.stream()
                .filter(g -> g.role.equals(config.getRoles().get(AccessControlEntry.ResourceType.TOPIC)))
                .map(g -> AKHQClaimResponseV3.Group.builder()
                        .role(config.getRoles().get(AccessControlEntry.ResourceType.SCHEMA))
                        .patterns(g.getPatterns())
                        .clusters(g.getClusters())
                        .build()
                ).toList());

        return AKHQClaimResponseV3.builder()
                .groups(result.isEmpty() ? null : Map.of("group", result))
                .build();
    }

    /**
     * Optimize the claim by merging bindings patterns with the same role and clusters, etc.
     *
     * @param bindings - the raw claim
     * @return an optimized claim
     */
    private List<AKHQClaimResponseV3.Group> optimizeV3Claim(Map<String, AKHQClaimResponseV3.Group> bindings) {
        List<AKHQClaimResponseV3.Group> result = new ArrayList<>();

        // Extract the clusters name from the managedClusters configuration
        List<String> clusters = managedClusters.stream().map(c -> String.format("^%s$", c.getName())).toList();

        bindings.forEach((key, value) -> {
            // Same pattern on all the clusters, we remove all the clusters and keep the *
            if (new HashSet<>(value.getClusters()).containsAll(clusters)) {
                value.setClusters(List.of("^.*$"));
            }
        });

        bindings.forEach((key, value) -> result.stream()
                // Search bindings with the same role and cluster filtering
                .filter(r -> {
                    List<String> c = new ArrayList<>(r.clusters);
                    c.removeAll(value.clusters);
                    // Same role and same clusters filtering
                    return r.role.equals(value.role) && c.isEmpty();
                })
                .findFirst()
                .ifPresentOrElse(
                        // If there is any we can merge the patterns and keep only 1 binding
                        toMerge -> toMerge.patterns.addAll(value.getPatterns()),
                        // Otherwise we add the current binding
                        () -> result.add(value)
                ));

        return result;
    }

    /**
     * List all the ACL for a user based on its LDAP groups
     * @param groups the user LDAP groups
     * @return the user's ACL
     */
    private List<AccessControlEntry> getAllAclForGroups(List<String> groups) {
        return namespaceService.listAll()
                .stream()
                .filter(namespace -> namespace.getMetadata().getLabels() != null &&
                        // Split by comma the groupLabel to support multiple groups and compare with user groups
                        !Collections.disjoint(groups,
                                List.of(namespace.getMetadata().getLabels()
                                        .getOrDefault(config.getGroupLabel(), "_")
                                        .split(","))))
                .flatMap(namespace -> accessControlEntryService.findAllGrantedToNamespace(namespace).stream())
                .collect(Collectors.toList());
    }

    /**
     * Compute AKHQ regexes from given ACLs
     * @param acls The ACLs
     * @param resourceType The resource type
     * @return A list of regex
     */
    public List<String> computeAllowedRegexListForResourceType(List<AccessControlEntry> acls, AccessControlEntry.ResourceType resourceType) {

        List<String> allowedRegex = acls.stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == resourceType)
                .filter(accessControlEntry ->
                        acls.stream()
                                .filter(accessControlEntryOther -> !accessControlEntryOther.getSpec().getResource().equals(accessControlEntry.getSpec().getResource()))
                                .map(accessControlEntryOther -> accessControlEntryOther.getSpec().getResource())
                                .noneMatch(escapedString -> accessControlEntry.getSpec().getResource().startsWith(escapedString)))
                .map(accessControlEntry -> {
                    String escapedString = Pattern.quote(accessControlEntry.getSpec().getResource());
                    if (accessControlEntry.getSpec().getResourcePatternType() == AccessControlEntry.ResourcePatternType.PREFIXED) {
                        return String.format("^%s.*$", escapedString);
                    } else {
                        return String.format("^%s$", escapedString);
                    }
                })
                .distinct()
                .toList();
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

        public static AKHQClaimResponse ofEmpty(List<String> roles) {
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

        public static AKHQClaimResponse ofAdmin(List<String> roles) {

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

        public static AKHQClaimResponseV2 ofEmpty(List<String> roles) {
            return AKHQClaimResponseV2.builder()
                    .roles(roles)
                    .topicsFilterRegexp(EMPTY_REGEXP)
                    .connectsFilterRegexp(EMPTY_REGEXP)
                    .consumerGroupsFilterRegexp(EMPTY_REGEXP)
                    .build();
        }

        public static AKHQClaimResponseV2 ofAdmin(List<String> roles) {

            return AKHQClaimResponseV2.builder()
                    .roles(roles)
                    .topicsFilterRegexp(ADMIN_REGEXP)
                    .connectsFilterRegexp(ADMIN_REGEXP)
                    .consumerGroupsFilterRegexp(ADMIN_REGEXP)
                    .build();
        }
    }

    @Introspected
    @Builder
    @Getter
    public static class AKHQClaimResponseV3 {
        private Map<String, List<Group>> groups;

        public static AKHQClaimResponseV3 ofAdmin(Map<AccessControlEntry.ResourceType, String> newAdminRoles) {
            return AKHQClaimResponseV3.builder()
                    .groups(Map.of("group",
                            newAdminRoles.values().stream()
                                    .map(r -> Group.builder().role(r).build()).collect(Collectors.toList())))
                    .build();
        }

        @Data
        @Builder
        @Introspected
        public static class Group {
            private String role;
            private List<String> patterns;
            private List<String> clusters;

        }
    }
}
