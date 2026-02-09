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
package com.michelin.ns4kafka.security.auth;

import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.RoleBindingService;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationResponse;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Authentication service. */
@Slf4j
@Singleton
public class AuthenticationService {
    private final ResourceBasedSecurityRule resourceBasedSecurityRule;
    private final RoleBindingService roleBindingService;
    private final Ns4KafkaProperties ns4KafkaProperties;

    /**
     * Constructor.
     *
     * @param resourceBasedSecurityRule The resource based security rule
     * @param ns4KafkaProperties The Ns4Kafka properties
     * @param roleBindingService The role binding service
     */
    public AuthenticationService(
            ResourceBasedSecurityRule resourceBasedSecurityRule,
            Ns4KafkaProperties ns4KafkaProperties,
            RoleBindingService roleBindingService) {
        this.resourceBasedSecurityRule = resourceBasedSecurityRule;
        this.ns4KafkaProperties = ns4KafkaProperties;
        this.roleBindingService = roleBindingService;
    }

    /**
     * Build an authentication response with the user details.
     *
     * @param username The username
     * @param groups The user groups
     * @return An authentication response with the user details
     */
    public AuthenticationResponse buildAuthJwtGroups(String username, List<String> groups) {
        List<RoleBinding> roleBindings = roleBindingService.findAllByGroups(groups);
        if (roleBindings.isEmpty()
                && groups.stream()
                        .noneMatch(group -> group.equalsIgnoreCase(
                                ns4KafkaProperties.getSecurity().getAdminGroup()))) {
            log.debug("Error during authentication: user groups not found in any namespace");
            throw new AuthenticationException(new AuthenticationFailed("No namespace matches your groups"));
        }

        return AuthenticationResponse.success(
                username,
                resourceBasedSecurityRule.computeRolesFromGroups(groups),
                Map.of(
                        ROLE_BINDINGS,
                        roleBindings.stream()
                                // group the namespaces by roles in a mapping
                                .collect(Collectors.groupingBy(
                                        roleBinding -> roleBinding.getSpec().getRole(),
                                        Collectors.mapping(
                                                roleBinding -> roleBinding
                                                        .getMetadata()
                                                        .getNamespace(),
                                                Collectors.toList())))
                                // build JWT with a list of namespaces for each different role
                                .entrySet()
                                .stream()
                                .map(entry -> AuthenticationRoleBinding.builder()
                                        .namespaces(entry.getValue())
                                        .verbs(new ArrayList<>(entry.getKey().getVerbs()))
                                        .resourceTypes(
                                                new ArrayList<>(entry.getKey().getResourceTypes()))
                                        .build())
                                .toList()));
    }
}
