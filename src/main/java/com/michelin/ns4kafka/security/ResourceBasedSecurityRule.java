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
package com.michelin.ns4kafka.security;

import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.security.auth.AuthenticationInfo;
import com.michelin.ns4kafka.security.auth.AuthenticationRoleBinding;
import com.michelin.ns4kafka.util.exception.ForbiddenNamespaceException;
import com.michelin.ns4kafka.util.exception.UnknownNamespaceException;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;
import io.micronaut.security.rules.SecurityRuleResult;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

/** Security rule to check if a user can access a given URL. */
@Slf4j
@Singleton
public class ResourceBasedSecurityRule implements SecurityRule<HttpRequest<?>> {
    public static final String IS_ADMIN = "isAdmin()";
    public static final String RESOURCE_PATTERN = "[a-zA-Z0-9_.-]";
    private final Pattern namespacedResourcePattern =
            Pattern.compile("^/api/namespaces/(?<namespace>" + RESOURCE_PATTERN + "+)" + "/(?<resourceType>[a-z_-]+)(/("
                    + RESOURCE_PATTERN + "+)(/(?<resourceSubtype>[a-z-]+))?)?$");

    private final Ns4KafkaProperties ns4KafkaProperties;
    private final NamespaceRepository namespaceRepository;

    /**
     * Constructor.
     *
     * @param ns4KafkaProperties The Ns4Kafka properties
     * @param namespaceRepository The namespace repository
     */
    public ResourceBasedSecurityRule(Ns4KafkaProperties ns4KafkaProperties, NamespaceRepository namespaceRepository) {
        this.ns4KafkaProperties = ns4KafkaProperties;
        this.namespaceRepository = namespaceRepository;
    }

    @Override
    public Publisher<SecurityRuleResult> check(
            @Nullable HttpRequest<?> request, @Nullable Authentication authentication) {
        return Publishers.just(checkSecurity(request, authentication));
    }

    /**
     * Check a user can access a given URL.
     *
     * @param request The current request
     * @param authentication The claims from the token
     * @return A security rule allowing the user or not
     */
    public SecurityRuleResult checkSecurity(HttpRequest<?> request, @Nullable Authentication authentication) {
        if (authentication == null) {
            return SecurityRuleResult.UNKNOWN;
        }

        if (!authentication.getAttributes().containsKey(ROLE_BINDINGS)) {
            log.debug("No authentication available for path [{}]. Returning unknown.", request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        // Request to a URL that is not in the scope of this SecurityRule
        Matcher matcher = namespacedResourcePattern.matcher(request.getPath());
        if (!matcher.find()) {
            log.debug("Invalid namespaced resource for path [{}]. Returning unknown.", request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        String namespace = matcher.group("namespace");
        String resourceSubtype = matcher.group("resourceSubtype");
        String resourceType;

        // Subresource handling ie. connects/restart or groups/reset
        if (StringUtils.isNotEmpty(resourceSubtype)) {
            resourceType = matcher.group("resourceType") + "/" + resourceSubtype;
        } else {
            resourceType = matcher.group("resourceType");
        }

        // Namespace doesn't exist
        String sub = authentication.getName();
        if (namespaceRepository.findByName(namespace).isEmpty()) {
            log.debug("Namespace not found for user \"{}\" on path \"{}\"", sub, request.getPath());
            throw new UnknownNamespaceException(namespace);
        }

        // Admin are allowed everything (provided that the namespace exists)
        Collection<String> roles = authentication.getRoles();
        if (roles.contains(IS_ADMIN)) {
            log.debug("Authorized admin \"{}\" on path \"{}\"", sub, request.getPath());
            return SecurityRuleResult.ALLOWED;
        }

        AuthenticationInfo authenticationInfo = AuthenticationInfo.of(authentication);

        // No role binding for the target namespace: the user is not allowed to access the target namespace
        List<AuthenticationRoleBinding> namespaceRoleBindings = authenticationInfo.getRoleBindings().stream()
                .filter(roleBinding -> roleBinding.getNamespaces().stream().anyMatch(ns -> ns.equals(namespace)))
                .toList();

        if (namespaceRoleBindings.isEmpty()) {
            log.debug(
                    "No matching role binding for user \"{}\" and namespace \"{}\" on path \"{}\"",
                    sub,
                    namespace,
                    request.getPath());
            throw new ForbiddenNamespaceException(namespace);
        }

        List<AuthenticationRoleBinding> authorizedRoleBindings = namespaceRoleBindings.stream()
                .filter(roleBinding -> roleBinding.getResourceTypes().contains(resourceType))
                .filter(roleBinding ->
                        roleBinding.getVerbs().contains(RoleBinding.Verb.valueOf(request.getMethodName())))
                .toList();

        // User not authorized to access requested resource
        if (authorizedRoleBindings.isEmpty()) {
            log.debug(
                    "No matching role binding for user \"{}\", namespace \"{}\", resource type \"{}\" "
                            + "and HTTP verb \"{}\" on path \"{}\"",
                    sub,
                    namespace,
                    resourceType,
                    request.getMethodName(),
                    request.getPath());
            return SecurityRuleResult.UNKNOWN;
        }

        if (log.isDebugEnabled()) {
            authorizedRoleBindings.forEach(
                    roleBinding -> log.debug("Found matching role binding \"{}\"", roleBinding.toString()));
            log.debug("Authorized user \"{}\" on path \"{}\"", sub, request.getPath());
        }

        return SecurityRuleResult.ALLOWED;
    }

    @Override
    public int getOrder() {
        return -1000;
    }

    /**
     * Compute roles from groups.
     *
     * @param groups The groups
     * @return The roles
     */
    public List<String> computeRolesFromGroups(List<String> groups) {
        List<String> roles = new ArrayList<>();

        if (groups.stream()
                .anyMatch(group ->
                        group.equalsIgnoreCase(ns4KafkaProperties.getSecurity().getAdminGroup()))) {
            roles.add(ResourceBasedSecurityRule.IS_ADMIN);
        }

        return roles;
    }
}
