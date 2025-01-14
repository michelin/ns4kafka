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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.security.authentication.Authentication;
import java.util.Collection;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

/**
 * Authentication info.
 * Type-safe representation of the authentication information and attributes.
 * This class can be injected in controllers to get the authenticated user information.
 *
 * @see <a href="https://micronaut-projects.github.io/micronaut-security/latest/guide/#customAuthenticatedUser">Micronaut Custom Binding</a>
 */
@Getter
@Builder
public class AuthenticationInfo {
    private String name;
    private Collection<String> roles;
    private Collection<AuthenticationRoleBinding> roleBindings;

    /**
     * Create an AuthenticationInfo from an Authentication.
     *
     * @param authentication the authentication
     * @return the authentication info
     */
    public static AuthenticationInfo of(Authentication authentication) {
        AuthenticationInfoBuilder builder = AuthenticationInfo.builder()
            .name(authentication.getName())
            .roles(authentication.getRoles());

        // Type-safe the role bindings attribute
        // JWT authentication role bindings attributes is a List of Map<String, String>
        // Basic authentication role bindings attributes is already a List<AuthenticationRoleBinding>
        // In all cases, convert attributes to avoid generic type or unchecked cast warnings
        List<?> roleBindings = (List<?>) authentication.getAttributes().get(ROLE_BINDINGS);
        ObjectMapper objectMapper = new ObjectMapper();
        List<AuthenticationRoleBinding> typedRoleBindings = roleBindings
            .stream()
            .map(roleBinding -> objectMapper.convertValue(roleBinding, AuthenticationRoleBinding.class))
            .toList();

        return builder
            .roleBindings(typedRoleBindings)
            .build();
    }
}
