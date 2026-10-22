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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.security.auth.AuthenticationInfo;
import com.michelin.ns4kafka.security.auth.AuthenticationRoleBinding;
import java.util.List;
import org.junit.jupiter.api.Test;

class ApiResourcesControllerTest {
    private final ApiResourcesController controller = new ApiResourcesController();

    @Test
    void shouldDiscoverTableflowAsAnIndependentNonReconciledResource() {
        var resources = controller.list(null);
        assertEquals(10, resources.size());
        var tableflow = resources.stream()
                .filter(resource -> resource.getKind().equals("TableflowTopic"))
                .findFirst()
                .orElseThrow();
        assertEquals("tableflow-topics", tableflow.getPath());
        assertTrue(tableflow.isNamespaced());
        assertFalse(tableflow.isSynchronizable());
    }

    @Test
    void shouldDiscoverOnlyResourcesGrantedToTheCaller() {
        var tableflowUser = AuthenticationInfo.builder()
                .roles(List.of())
                .roleBindings(List.of(AuthenticationRoleBinding.builder()
                        .resourceTypes(List.of("tableflow-topics"))
                        .build()))
                .build();
        var topicUser = AuthenticationInfo.builder()
                .roles(List.of())
                .roleBindings(List.of(AuthenticationRoleBinding.builder()
                        .resourceTypes(List.of("topics"))
                        .build()))
                .build();

        assertEquals(
                List.of(ApiResourcesController.NAMESPACE, ApiResourcesController.TABLEFLOW_TOPIC),
                controller.list(tableflowUser));
        assertEquals(
                List.of(ApiResourcesController.TOPIC, ApiResourcesController.NAMESPACE), controller.list(topicUser));
    }
}
