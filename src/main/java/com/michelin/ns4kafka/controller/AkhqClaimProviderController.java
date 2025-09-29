/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.controller;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.NamespaceService;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

/** Controller to manage AKHQ claims. */
@Tag(name = "AKHQ", description = "Manage the AKHQ endpoints.")
@RolesAllowed(SecurityRule.IS_ANONYMOUS)
@Controller("/akhq-claim")
public class AkhqClaimProviderController {
    private static final String TOPICS_FILTER_REGEX = "topicsFilterRegexp";
    private static final String CONNECTS_FILTER_REGEX = "connectsFilterRegexp";
    private static final String CONSUMER_GROUPS_FILTER_REGEX = "consumerGroupsFilterRegexp";
    private static final List<String> EMPTY_REGEXP = List.of("^none$");
    private static final List<String> ADMIN_REGEXP = List.of(".*");

    @Inject
    private Ns4KafkaProperties ns4KafkaProperties;

    @Inject
    private AclService aclService;

    @Inject
    private NamespaceService namespaceService;

    @Inject
    private List<ManagedClusterProperties> managedClusters;

    /**
     * List AKHQ claims (v019 and prior).
     *
     * @param request The AKHQ request
     * @return The AKHQ claims
     */
    @Post
    public AkhqClaimResponse generateClaim(@Valid @Body AkhqClaimRequest request) {
        if (request == null) {
            return AkhqClaimResponse.ofEmpty(ns4KafkaProperties.getAkhq().getFormerRoles());
        }

        final List<String> groups = Optional.ofNullable(request.getGroups()).orElse(new ArrayList<>());

        if (groups.contains(ns4KafkaProperties.getAkhq().getAdminGroup())) {
            return AkhqClaimResponse.ofAdmin(ns4KafkaProperties.getAkhq().getFormerAdminRoles());
        }

        List<AccessControlEntry> relatedAcl = namespaceService.findAll().stream()
                .filter(namespace -> namespace.getMetadata().getLabels() != null
                        && groups.contains(namespace
                                .getMetadata()
                                .getLabels()
                                .getOrDefault(ns4KafkaProperties.getAkhq().getGroupLabel(), "_")))
                .flatMap(namespace -> aclService.findAllGrantedToNamespace(namespace).stream())
                .collect(Collectors.toList());

        // Add all public ACLs.
        relatedAcl.addAll(aclService.findAllPublicGrantedTo());

        return AkhqClaimResponse.builder()
                .roles(ns4KafkaProperties.getAkhq().getFormerRoles())
                .attributes(Map.of(
                        TOPICS_FILTER_REGEX,
                        computeAllowedRegexListForResourceType(relatedAcl, AccessControlEntry.ResourceType.TOPIC),
                        CONNECTS_FILTER_REGEX,
                        computeAllowedRegexListForResourceType(relatedAcl, AccessControlEntry.ResourceType.CONNECT),
                        CONSUMER_GROUPS_FILTER_REGEX,
                        ADMIN_REGEXP))
                .build();
    }

    /**
     * List AKHQ claims (v020 to 024).
     *
     * @param request The AKHQ request
     * @return The AKHQ claims
     */
    @Post("/v2")
    public AkhqClaimResponseV2 generateClaimV2(@Valid @Body AkhqClaimRequest request) {
        if (request == null) {
            return AkhqClaimResponseV2.ofEmpty(ns4KafkaProperties.getAkhq().getFormerRoles());
        }

        final List<String> groups = Optional.ofNullable(request.getGroups()).orElse(new ArrayList<>());

        if (groups.contains(ns4KafkaProperties.getAkhq().getAdminGroup())) {
            return AkhqClaimResponseV2.ofAdmin(ns4KafkaProperties.getAkhq().getFormerAdminRoles());
        }

        List<AccessControlEntry> relatedAcl = getAclsByGroups(groups);

        // Add all public ACLs.
        relatedAcl.addAll(aclService.findAllPublicGrantedTo());

        return AkhqClaimResponseV2.builder()
                .roles(ns4KafkaProperties.getAkhq().getFormerRoles())
                .topicsFilterRegexp(
                        computeAllowedRegexListForResourceType(relatedAcl, AccessControlEntry.ResourceType.TOPIC))
                .connectsFilterRegexp(
                        computeAllowedRegexListForResourceType(relatedAcl, AccessControlEntry.ResourceType.CONNECT))
                .consumerGroupsFilterRegexp(ADMIN_REGEXP)
                .build();
    }

    /**
     * List AKHQ claims (v025 and higher).
     *
     * @param request The AKHQ request
     * @return The AKHQ claims
     */
    @Post("/v3")
    public AkhqClaimResponseV3 generateClaimV3(@Valid @Body AkhqClaimRequest request) {
        final List<String> groups = Optional.ofNullable(request.getGroups()).orElse(new ArrayList<>());

        if (groups.contains(ns4KafkaProperties.getAkhq().getAdminGroup())) {
            return AkhqClaimResponseV3.ofAdmin(ns4KafkaProperties.getAkhq().getAdminRoles());
        }

        List<AccessControlEntry> acls = getAclsByGroups(groups);

        // Add all public ACLs
        acls.addAll(aclService.findAllPublicGrantedTo());

        // Remove unnecessary ACLs
        // E.g., project.topic1 when project.* is granted on the same resource type and cluster
        optimizeAcl(acls);

        Map<String, AkhqClaimResponseV3.Group> bindings = new LinkedHashMap<>();

        // Start by creating a map that store permissions by role/cluster
        acls.forEach(acl -> {
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

            String role =
                    ns4KafkaProperties.getAkhq().getRoles().get(acl.getSpec().getResourceType());
            String key = role + "-" + acl.getSpec().getResource() + "-"
                    + acl.getSpec().getResourcePatternType();

            // If we already have permissions for the role and cluster, add the pattern to the existing one
            if (bindings.containsKey(key)) {
                bindings.get(key).getClusters().add(patternCluster);
            } else {
                List<String> regexes = new ArrayList<>();
                regexes.add(patternRegex);
                List<String> clusters = new ArrayList<>();
                clusters.add(patternCluster);

                // Otherwise we add a new one
                bindings.put(
                        key,
                        AkhqClaimResponseV3.Group.builder()
                                .role(role)
                                .patterns(regexes)
                                .clusters(clusters)
                                .build());
            }
        });

        List<AkhqClaimResponseV3.Group> result = optimizeV3Claim(bindings);

        // Add the same pattern and cluster filtering for SCHEMA as the TOPIC ones
        result.addAll(result.stream()
                .filter(g -> g.role.equals(
                        ns4KafkaProperties.getAkhq().getRoles().get(AccessControlEntry.ResourceType.TOPIC)))
                .map(g -> {
                    // Takes all the PREFIXED patterns as-is
                    List<String> patterns = new ArrayList<>(g.getPatterns().stream()
                            .filter(p -> p.endsWith("\\E.*$"))
                            .toList());

                    // Add -key or -value prefix to the schema pattern for LITERAL patterns
                    patterns.addAll(g.getPatterns().stream()
                            .filter(p -> p.endsWith("\\E$"))
                            .map(p -> p.replace("\\E$", "-\\E(key|value)$"))
                            .toList());

                    return AkhqClaimResponseV3.Group.builder()
                            .role(ns4KafkaProperties.getAkhq().getRoles().get(AccessControlEntry.ResourceType.SCHEMA))
                            .patterns(patterns)
                            .clusters(g.getClusters())
                            .build();
                })
                .toList());

        // If "GROUP" claim is present, remove the patterns to allow all the groups
        result.stream()
                .filter(group -> group.getRole()
                        .equals(ns4KafkaProperties.getAkhq().getRoles().get(AccessControlEntry.ResourceType.GROUP)))
                .findFirst()
                .ifPresent(group -> group.setPatterns(null));

        return AkhqClaimResponseV3.builder()
                .groups(result.isEmpty() ? null : Map.of("group", result))
                .build();
    }

    /**
     * Remove ACL that are already included by another ACL on the same resource and cluster. Ex: LITERAL ACL1 with
     * project.topic1 resource + PREFIXED ACL2 with project -> return ACL2 only
     *
     * @param acls the input list of acl to optimize
     */
    private void optimizeAcl(List<AccessControlEntry> acls) {
        acls.removeIf(acl -> acls.stream()
                .anyMatch(aclOther ->
                        // Not comparing the ACL with itself
                        !aclOther.getMetadata()
                                        .getName()
                                        .equals(acl.getMetadata().getName())
                                // Check PREFIXED ACL on the same resource type and cluster
                                && aclOther.getSpec()
                                        .getResourcePatternType()
                                        .equals(AccessControlEntry.ResourcePatternType.PREFIXED)
                                && aclOther.getSpec()
                                        .getResourceType()
                                        .equals(acl.getSpec().getResourceType())
                                && aclOther.getMetadata()
                                        .getCluster()
                                        .equals(acl.getMetadata().getCluster())
                                // Check the resource is included in another acl
                                && acl.getSpec()
                                        .getResource()
                                        .startsWith(aclOther.getSpec().getResource())));
    }

    /**
     * Optimize the claim by merging bindings patterns with the same role and clusters, etc.
     *
     * @param bindings the raw claim
     * @return an optimized claim
     */
    private List<AkhqClaimResponseV3.Group> optimizeV3Claim(Map<String, AkhqClaimResponseV3.Group> bindings) {
        List<AkhqClaimResponseV3.Group> result = new ArrayList<>();

        // Extract the clusters name from the managedClusters configuration
        List<String> clusters = managedClusters.stream()
                .map(c -> String.format("^%s$", c.getName()))
                .toList();

        bindings.forEach((key, value) -> {
            // Same pattern on all the clusters, we remove all the clusters and keep the *
            if (new HashSet<>(value.getClusters()).containsAll(clusters)) {
                value.setClusters(List.of("^.*$"));
            }
        });

        bindings.forEach((key, value) -> result.stream()
                // Search bindings with the same role and cluster filtering
                .filter(r -> r.role.equals(value.role)
                        && r.clusters.size() == value.clusters.size()
                        && new HashSet<>(r.clusters).containsAll(value.clusters)
                        && new HashSet<>(value.clusters).containsAll(r.clusters))
                .findFirst()
                .ifPresentOrElse(
                        // If there is any we can merge the patterns and keep only 1 binding
                        toMerge -> toMerge.patterns.addAll(value.getPatterns()),
                        // Otherwise we add the current binding
                        () -> result.add(value)));

        return result;
    }

    /**
     * List all the ACL for a user based on its LDAP groups.
     *
     * @param groups the user LDAP groups
     * @return the user's ACL
     */
    private List<AccessControlEntry> getAclsByGroups(List<String> groups) {
        return namespaceService.findAll().stream()
                .filter(namespace -> namespace.getMetadata().getLabels() != null
                        // Split the namespace groups by the groupDelimiter to support multiple groups and compare with
                        // the user groups
                        && !Collections.disjoint(
                                groups,
                                List.of(namespace
                                        .getMetadata()
                                        .getLabels()
                                        .getOrDefault(
                                                ns4KafkaProperties.getAkhq().getGroupLabel(), "_")
                                        .split(ns4KafkaProperties.getAkhq().getGroupDelimiter()))))
                .flatMap(namespace -> aclService.findAllGrantedToNamespace(namespace).stream())
                .collect(Collectors.toList());
    }

    /**
     * Compute AKHQ regexes from given ACLs.
     *
     * @param acls The ACLs
     * @param resourceType The resource type
     * @return A list of regex
     */
    public List<String> computeAllowedRegexListForResourceType(
            List<AccessControlEntry> acls, AccessControlEntry.ResourceType resourceType) {

        List<String> allowedRegex = acls.stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == resourceType)
                .filter(accessControlEntry -> acls.stream()
                        .filter(accessControlEntryOther -> !accessControlEntryOther
                                .getSpec()
                                .getResource()
                                .equals(accessControlEntry.getSpec().getResource()))
                        .map(accessControlEntryOther ->
                                accessControlEntryOther.getSpec().getResource())
                        .noneMatch(escapedString ->
                                accessControlEntry.getSpec().getResource().startsWith(escapedString)))
                .map(accessControlEntry -> {
                    String escapedString =
                            Pattern.quote(accessControlEntry.getSpec().getResource());
                    if (accessControlEntry.getSpec().getResourcePatternType()
                            == AccessControlEntry.ResourcePatternType.PREFIXED) {
                        return String.format("^%s.*$", escapedString);
                    } else {
                        return String.format("^%s$", escapedString);
                    }
                })
                .distinct()
                .toList();
        // AKHQ considers empty list as "^.*$" so we must return something
        return !allowedRegex.isEmpty() ? allowedRegex : EMPTY_REGEXP;
    }

    /** AKHQ request. */
    @Introspected
    @Builder
    @Getter
    public static class AkhqClaimRequest {
        String providerType;
        String providerName;
        String username;
        List<String> groups;
    }

    /** AKHQ response. */
    @Introspected
    @Builder
    @Getter
    public static class AkhqClaimResponse {
        private List<String> roles;
        private Map<String, List<String>> attributes;

        /**
         * Build an empty AKHQ response.
         *
         * @param roles the roles
         * @return the AKHQ response
         */
        public static AkhqClaimResponse ofEmpty(List<String> roles) {
            return AkhqClaimResponse.builder()
                    .roles(roles)
                    .attributes(Map.of(
                            // AKHQ considers empty list as "^.*$" so we must return something
                            TOPICS_FILTER_REGEX, EMPTY_REGEXP,
                            CONNECTS_FILTER_REGEX, EMPTY_REGEXP,
                            CONSUMER_GROUPS_FILTER_REGEX, EMPTY_REGEXP))
                    .build();
        }

        /**
         * Build an AKHQ response for an admin.
         *
         * @param roles the roles
         * @return the AKHQ response
         */
        public static AkhqClaimResponse ofAdmin(List<String> roles) {
            return AkhqClaimResponse.builder()
                    .roles(roles)
                    .attributes(Map.of(
                            // AKHQ considers empty list as "^.*$" so we must return something
                            TOPICS_FILTER_REGEX, ADMIN_REGEXP,
                            CONNECTS_FILTER_REGEX, ADMIN_REGEXP,
                            CONSUMER_GROUPS_FILTER_REGEX, ADMIN_REGEXP))
                    .build();
        }
    }

    /** AKHQ response (v2). */
    @Introspected
    @Builder
    @Getter
    public static class AkhqClaimResponseV2 {
        private List<String> roles;
        private List<String> topicsFilterRegexp;
        private List<String> connectsFilterRegexp;
        private List<String> consumerGroupsFilterRegexp;

        /**
         * Build an empty AKHQ response.
         *
         * @param roles the roles
         * @return the AKHQ response
         */
        public static AkhqClaimResponseV2 ofEmpty(List<String> roles) {
            return AkhqClaimResponseV2.builder()
                    .roles(roles)
                    .topicsFilterRegexp(EMPTY_REGEXP)
                    .connectsFilterRegexp(EMPTY_REGEXP)
                    .consumerGroupsFilterRegexp(EMPTY_REGEXP)
                    .build();
        }

        /**
         * Build an AKHQ response for an admin.
         *
         * @param roles the roles
         * @return the AKHQ response
         */
        public static AkhqClaimResponseV2 ofAdmin(List<String> roles) {
            return AkhqClaimResponseV2.builder()
                    .roles(roles)
                    .topicsFilterRegexp(ADMIN_REGEXP)
                    .connectsFilterRegexp(ADMIN_REGEXP)
                    .consumerGroupsFilterRegexp(ADMIN_REGEXP)
                    .build();
        }
    }

    /** AKHQ response (v3). */
    @Introspected
    @Builder
    @Getter
    public static class AkhqClaimResponseV3 {
        private Map<String, List<Group>> groups;

        /**
         * Build an AKHQ response for an admin.
         *
         * @param newAdminRoles the roles
         * @return the AKHQ response
         */
        public static AkhqClaimResponseV3 ofAdmin(Map<AccessControlEntry.ResourceType, String> newAdminRoles) {
            return AkhqClaimResponseV3.builder()
                    .groups(Map.of(
                            "group",
                            newAdminRoles.values().stream()
                                    .map(r -> Group.builder().role(r).build())
                                    .collect(Collectors.toList())))
                    .build();
        }

        /** AKHQ group. */
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
