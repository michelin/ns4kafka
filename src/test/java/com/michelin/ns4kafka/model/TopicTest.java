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

import java.util.Map;
import org.junit.jupiter.api.Test;

class TopicTest {
    @Test
    void shouldBeEqual() {
        Topic original = Topic.builder()
                .metadata(Metadata.builder().name("topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("k1", "v1", "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        Topic same = Topic.builder()
                .metadata(Metadata.builder().name("topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("k1", "v1", "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofSuccess("Created !"))
                .build();

        assertEquals(original, same);

        Topic differentByMetadata = Topic.builder()
                .metadata(Metadata.builder().name("topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("k1", "v1", "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        assertNotEquals(original, differentByMetadata);

        Topic differentByReplicationFactor = Topic.builder()
                .metadata(Metadata.builder().name("topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(99)
                        .partitions(3)
                        .configs(Map.of("k1", "v1", "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        assertNotEquals(original, differentByReplicationFactor);

        Topic differentByPartitions = Topic.builder()
                .metadata(Metadata.builder().name("topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(99)
                        .configs(Map.of("k1", "v1", "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        assertNotEquals(original, differentByPartitions);

        Topic differentByConfigs = Topic.builder()
                .metadata(Metadata.builder().name("topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("k1", "v1"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        assertNotEquals(original, differentByConfigs);
    }
}
