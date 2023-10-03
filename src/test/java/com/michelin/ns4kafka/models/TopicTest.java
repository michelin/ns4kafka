package com.michelin.ns4kafka.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TopicTest {
    @Test
    void testEquals() {
        Topic original = Topic.builder()
            .metadata(ObjectMeta.builder().name("topic1").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Topic.TopicStatus.ofPending())
            .build();

        Topic same = Topic.builder()
            .metadata(ObjectMeta.builder().name("topic1").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Topic.TopicStatus.ofSuccess("Created !"))
            .build();

        assertEquals(original, same);

        Topic differentByMetadata = Topic.builder()
            .metadata(ObjectMeta.builder().name("topic2").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Topic.TopicStatus.ofPending())
            .build();

        assertNotEquals(original, differentByMetadata);

        Topic differentByReplicationFactor = Topic.builder()
            .metadata(ObjectMeta.builder().name("topic2").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(99)
                .partitions(3)
                .configs(Map.of("k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Topic.TopicStatus.ofPending())
            .build();

        assertNotEquals(original, differentByReplicationFactor);

        Topic differentByPartitions = Topic.builder()
            .metadata(ObjectMeta.builder().name("topic2").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(99)
                .configs(Map.of("k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Topic.TopicStatus.ofPending())
            .build();

        assertNotEquals(original, differentByPartitions);

        Topic differentByConfigs = Topic.builder()
            .metadata(ObjectMeta.builder().name("topic2").build())
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
