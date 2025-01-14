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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controller.connect.ConnectClusterNonNamespacedController;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.service.ConnectClusterService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ConnectClusterNonNamespacedControllerTest {
    @Mock
    ConnectClusterService connectClusterService;

    @InjectMocks
    ConnectClusterNonNamespacedController connectClusterNonNamespacedController;

    @Test
    void shouldListAll() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("connect-cluster")
                .build())
            .build();

        when(connectClusterService.findAll(anyBoolean())).thenReturn(Flux.fromIterable(List.of(connectCluster)));

        StepVerifier.create(connectClusterNonNamespacedController.listAll(false))
            .consumeNextWith(result -> assertEquals("connect-cluster", result.getMetadata().getName()))
            .verifyComplete();
    }
}
