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

package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.michelin.ns4kafka.model.connector.Connector;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorTest {
    @Test
    void shouldBeEqual() {
        Connector original = Connector.builder()
            .metadata(Metadata.builder()
                .name("connect1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster1")
                .config(Map.of(
                    "k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.RUNNING)
                .build())
            .build();

        Connector same = Connector.builder()
            .metadata(Metadata.builder()
                .name("connect1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster1")
                // inverted map
                .config(Map.of(
                    "k2", "v2",
                    "k1", "v1"))
                .build())
            // different status
            .status(Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.FAILED)
                .build())
            .build();

        Connector differentByConnectCluster = Connector.builder()
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster2")
                .config(Map.of(
                    "k1", "v1",
                    "k2", "v2"))
                .build())
            .build();

        Connector differentByConfig = Connector.builder()
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster2")
                .config(Map.of(
                    "k1", "v1",
                    "k2", "v2",
                    "k3", "v3"))
                .build())
            .build();

        // objects are same, even if status differs
        assertEquals(original, same);

        assertNotEquals(original, differentByConnectCluster);
        assertNotEquals(original, differentByConfig);

        Connector differentByMetadata = Connector.builder()
            .metadata(Metadata.builder()
                .name("connect2")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster1")
                .config(Map.of("k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.RUNNING)
                .build())
            .build();

        assertNotEquals(original, differentByMetadata);
    }
}
