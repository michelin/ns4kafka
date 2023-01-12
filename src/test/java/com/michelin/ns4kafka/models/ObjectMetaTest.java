package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

class ObjectMetaTest {
    @Test
    void testEquals() {
        ObjectMeta original = ObjectMeta.builder()
                .name("name1")
                .namespace("namespace1")
                .cluster("local")
                .labels(Map.of("key1", "val1",
                        "key2", "val2"))
                .creationTimestamp(Date.from(Instant.now()))
                .generation(0)
                .build();
        ObjectMeta same = ObjectMeta.builder()
                .name("name1")
                .namespace("namespace1")
                .cluster("local")
                // inverted map order
                .labels(Map.of("key2", "val2",
                        "key1", "val1"))
                // different date
                .creationTimestamp(Date.from(Instant.now().plusMillis(1000)))
                // different gen
                .generation(99)
                .build();
        ObjectMeta different = ObjectMeta.builder()
                .name("name2")
                .namespace("namespace1")
                .cluster("local")
                .labels(Map.of())
                .creationTimestamp(Date.from(Instant.now()))
                .generation(0)
                .build();
        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(original, different);
    }
}
