package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class TopicTest {
    @Test
    void testEquals(){
        Topic original = Topic.builder()
                .metadata(ObjectMeta.builder().name("topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("k1","v1",
                                "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        Topic same = Topic.builder()
                .metadata(ObjectMeta.builder().name("topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("k1","v1",
                                "k2", "v2"))
                        .build())
                // Status should not intefere
                .status(Topic.TopicStatus.ofSuccess("Created !"))
                .build();

        Topic differentByMetadata = Topic.builder()
                .metadata(ObjectMeta.builder().name("topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("k1","v1",
                                "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        Topic differentByReplicationFactor = Topic.builder()
                .metadata(ObjectMeta.builder().name("topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(99)
                        .partitions(3)
                        .configs(Map.of("k1","v1",
                                "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        Topic differentByPartitions = Topic.builder()
                .metadata(ObjectMeta.builder().name("topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(99)
                        .configs(Map.of("k1","v1",
                                "k2", "v2"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        Topic differentByConfigs = Topic.builder()
                .metadata(ObjectMeta.builder().name("topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .configs(Map.of("k1","v1"))
                        .build())
                .status(Topic.TopicStatus.ofPending())
                .build();

        Assertions.assertEquals(original,same);

        Assertions.assertNotEquals(original, differentByMetadata);
        Assertions.assertNotEquals(original, differentByReplicationFactor);
        Assertions.assertNotEquals(original, differentByPartitions);
        Assertions.assertNotEquals(original, differentByConfigs);
    }
}
