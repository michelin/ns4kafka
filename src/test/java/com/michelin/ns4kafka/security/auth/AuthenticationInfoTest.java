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

import static com.michelin.ns4kafka.model.RoleBinding.Verb.GET;
import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLE_BINDINGS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.micronaut.security.authentication.Authentication;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthenticationInfoTest {

    @Test
    void shouldConvertFromMapRoleBindingsType() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ROLE_BINDINGS, List.of(Map.of("namespaces", List.of("namespace"),
            "verbs", List.of("GET"),
            "resourceTypes", List.of("topics"))));
        Authentication authentication = Authentication.build("name", List.of("role"), attributes);

        AuthenticationInfo authenticationInfo = AuthenticationInfo.of(authentication);

        assertEquals("name", authenticationInfo.getName());
        assertEquals(List.of("role"), authenticationInfo.getRoles().stream().toList());
        assertIterableEquals(List.of(new AuthenticationRoleBinding(List.of("namespace"), List.of(GET),
            List.of("topics"))), authenticationInfo.getRoleBindings().stream().toList());
    }

    @Test
    void shouldConvertFromAuthenticationRoleBindingsType() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ROLE_BINDINGS, List.of(new AuthenticationRoleBinding(List.of("namespace"), List.of(GET),
            List.of("topics"))));
        Authentication authentication = Authentication.build("name", List.of("role"), attributes);

        AuthenticationInfo authenticationInfo = AuthenticationInfo.of(authentication);

        assertEquals("name", authenticationInfo.getName());
        assertEquals(List.of("role"), authenticationInfo.getRoles().stream().toList());
        assertIterableEquals(List.of(new AuthenticationRoleBinding(List.of("namespace"), List.of(GET),
            List.of("topics"))), authenticationInfo.getRoleBindings().stream().toList());
    }
}
