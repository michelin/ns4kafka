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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.property.SecurityProperties;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.RoleBindingService;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationResponse;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthenticationServiceTest {
    @Mock
    RoleBindingService roleBindingService;

    @Mock
    SecurityProperties securityProperties;

    @Mock
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @InjectMocks
    AuthenticationService authenticationService;

    @Test
    void shouldThrowErrorWhenNoRoleBindingAndNotAdmin() {
        when(roleBindingService.findAllByGroups(any())).thenReturn(Collections.emptyList());

        when(securityProperties.getAdminGroup()).thenReturn("admin");

        List<String> groups = List.of("group");
        AuthenticationException exception = assertThrows(
                AuthenticationException.class, () -> authenticationService.buildAuthJwtGroups("username", groups));

        assertTrue(exception.getResponse().getMessage().isPresent());
        assertEquals(
                "No namespace matches your groups",
                exception.getResponse().getMessage().get());
    }

    @Test
    void shouldReturnAuthenticationSuccessWhenAdminNoGroup() {
        when(roleBindingService.findAllByGroups(any())).thenReturn(Collections.emptyList());

        when(securityProperties.getAdminGroup()).thenReturn("admin");

        when(resourceBasedSecurityRule.computeRolesFromGroups(any()))
                .thenReturn(List.of(ResourceBasedSecurityRule.IS_ADMIN));

        AuthenticationResponse response = authenticationService.buildAuthJwtGroups("admin", List.of("admin"));

        assertTrue(response.getAuthentication().isPresent());
        assertEquals("admin", response.getAuthentication().get().getName());
        assertTrue(response.getAuthentication().get().getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN));
        assertTrue(response.getAuthentication().get().getAttributes().containsKey(ROLE_BINDINGS));
        assertTrue(((List<?>) response.getAuthentication().get().getAttributes().get(ROLE_BINDINGS)).isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnAuthenticationSuccessWhenAdminWithGroups() {
        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(Metadata.builder().name("ns1-rb").namespace("ns1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        when(roleBindingService.findAllByGroups(any())).thenReturn(List.of(roleBinding));

        when(resourceBasedSecurityRule.computeRolesFromGroups(any()))
                .thenReturn(List.of(ResourceBasedSecurityRule.IS_ADMIN));

        AuthenticationResponse response = authenticationService.buildAuthJwtGroups("admin", List.of("admin"));

        assertTrue(response.getAuthentication().isPresent());
        assertEquals("admin", response.getAuthentication().get().getName());
        assertTrue(response.getAuthentication().get().getRoles().contains(ResourceBasedSecurityRule.IS_ADMIN));
        assertTrue(response.getAuthentication().get().getAttributes().containsKey(ROLE_BINDINGS));
        assertEquals(
                List.of("ns1"),
                ((List<AuthenticationRoleBinding>) response.getAuthentication()
                                .get()
                                .getAttributes()
                                .get(ROLE_BINDINGS))
                        .getFirst()
                        .getNamespaces());
        assertTrue(((List<AuthenticationRoleBinding>)
                        response.getAuthentication().get().getAttributes().get(ROLE_BINDINGS))
                .getFirst()
                .getVerbs()
                .containsAll(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET)));
        assertTrue(((List<AuthenticationRoleBinding>)
                        response.getAuthentication().get().getAttributes().get(ROLE_BINDINGS))
                .getFirst()
                .getResourceTypes()
                .containsAll(List.of("topics", "acls")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnAuthenticationSuccessWhenUserWithGroups() {
        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(Metadata.builder().name("ns1-rb").namespace("ns1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        when(roleBindingService.findAllByGroups(any())).thenReturn(List.of(roleBinding));

        when(resourceBasedSecurityRule.computeRolesFromGroups(any())).thenReturn(List.of());

        AuthenticationResponse response = authenticationService.buildAuthJwtGroups("user", List.of("group"));

        assertTrue(response.getAuthentication().isPresent());
        assertEquals("user", response.getAuthentication().get().getName());
        assertTrue(response.getAuthentication().get().getRoles().isEmpty());
        assertTrue(response.getAuthentication().get().getAttributes().containsKey(ROLE_BINDINGS));
        assertEquals(
                List.of("ns1"),
                ((List<AuthenticationRoleBinding>) response.getAuthentication()
                                .get()
                                .getAttributes()
                                .get(ROLE_BINDINGS))
                        .getFirst()
                        .getNamespaces());
        assertTrue(((List<AuthenticationRoleBinding>)
                        response.getAuthentication().get().getAttributes().get(ROLE_BINDINGS))
                .getFirst()
                .getVerbs()
                .containsAll(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET)));
        assertTrue(((List<AuthenticationRoleBinding>)
                        response.getAuthentication().get().getAttributes().get(ROLE_BINDINGS))
                .getFirst()
                .getResourceTypes()
                .containsAll(List.of("topics", "acls")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnAuthenticationSuccessWhenMultipleGroupsWithSameVerbsAndResourceTypes() {
        RoleBinding roleBinding1 = RoleBinding.builder()
                .metadata(Metadata.builder().name("ns1-rb").namespace("ns1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        RoleBinding roleBinding2 = RoleBinding.builder()
                .metadata(Metadata.builder().name("ns2-rb").namespace("ns2").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics"))
                                .verbs(List.of(RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group2")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        RoleBinding roleBinding3 = RoleBinding.builder()
                .metadata(Metadata.builder().name("ns3-rb").namespace("ns3").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group3")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        when(roleBindingService.findAllByGroups(any())).thenReturn(List.of(roleBinding1, roleBinding2, roleBinding3));

        when(resourceBasedSecurityRule.computeRolesFromGroups(any())).thenReturn(List.of());

        AuthenticationResponse response = authenticationService.buildAuthJwtGroups("user", List.of("group1"));

        assertTrue(response.getAuthentication().isPresent());
        assertEquals("user", response.getAuthentication().get().getName());
        assertTrue(response.getAuthentication().get().getRoles().isEmpty());
        assertTrue(response.getAuthentication().get().getAttributes().containsKey(ROLE_BINDINGS));
        assertTrue(((List<AuthenticationRoleBinding>)
                        response.getAuthentication().get().getAttributes().get(ROLE_BINDINGS))
                .containsAll(List.of(
                        AuthenticationRoleBinding.builder()
                                .namespaces(List.of("ns1", "ns3"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .resourceTypes(List.of("topics", "acls"))
                                .build(),
                        AuthenticationRoleBinding.builder()
                                .namespaces(List.of("ns2"))
                                .verbs(List.of(RoleBinding.Verb.GET))
                                .resourceTypes(List.of("topics"))
                                .build())));
    }
}
