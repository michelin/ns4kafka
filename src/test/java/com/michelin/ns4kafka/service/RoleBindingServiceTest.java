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
package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.repository.RoleBindingRepository;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RoleBindingServiceTest {
    @Mock
    RoleBindingRepository roleBindingRepository;

    @InjectMocks
    RoleBindingService roleBindingService;

    @Test
    void shouldFindByName() {
        RoleBinding rb1 = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("namespace-rb1")
                        .cluster("local")
                        .build())
                .build();

        when(roleBindingRepository.findByName(any(), any())).thenReturn(Optional.of(rb1));

        var result = roleBindingService.findByName("namespace", "namespace-rb2");

        assertTrue(result.isPresent());
        assertEquals(rb1, result.get());
    }

    @Test
    void shouldListRoleBindingsWithoutParameter() {
        RoleBinding rb1 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb1").build())
                .build();

        RoleBinding rb2 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb2").build())
                .build();

        RoleBinding rb3 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb3").build())
                .build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3));

        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.findAllForNamespace("namespace"));
    }

    @Test
    void shouldListRoleBindingsWithNameParameter() {
        RoleBinding rb1 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb1").build())
                .build();

        RoleBinding rb2 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb2").build())
                .build();

        RoleBinding rb3 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb3").build())
                .build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3));

        assertEquals(List.of(rb1), roleBindingService.findByWildcardName("namespace", "namespace-rb1"));
        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.findByWildcardName("namespace", ""));
        assertEquals(List.of(), roleBindingService.findByWildcardName("namespace", "namespace-rb5"));
    }

    @Test
    void shouldListRoleBindingsWithWildcardNameParameter() {
        RoleBinding rb1 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb1").build())
                .build();

        RoleBinding rb2 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb2").build())
                .build();

        RoleBinding rb3 = RoleBinding.builder()
                .metadata(Metadata.builder().name("namespace-rb3").build())
                .build();

        RoleBinding rb4 = RoleBinding.builder()
                .metadata(Metadata.builder().name("rb4").build())
                .build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1, rb2, rb3, rb4));

        assertEquals(List.of(rb1, rb2, rb3), roleBindingService.findByWildcardName("namespace", "namespace-*"));
        assertEquals(List.of(rb1, rb2, rb3, rb4), roleBindingService.findByWildcardName("namespace", "*rb?"));
        assertEquals(List.of(rb4), roleBindingService.findByWildcardName("namespace", "rb?"));
        assertEquals(List.of(), roleBindingService.findByWildcardName("namespace", "role_binding*"));
    }
}
