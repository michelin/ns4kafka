package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MetadataTest {
    @Test
    void shouldBeEqual() {
        Metadata original = Metadata.builder()
            .name("name1")
            .namespace("namespace1")
            .cluster("local")
            .labels(Map.of(
                "key1", "val1",
                "key2", "val2"))
            .creationTimestamp(Date.from(Instant.now()))
            .generation(0)
            .build();
        Metadata same = Metadata.builder()
            .name("name1")
            .namespace("namespace1")
            .cluster("local")
            // inverted map order
            .labels(Map.of(
                "key2", "val2",
                "key1", "val1"))
            // different date
            .creationTimestamp(Date.from(Instant.now().plusMillis(1000)))
            // different gen
            .generation(99)
            .build();
        Metadata different = Metadata.builder()
            .name("name2")
            .namespace("namespace1")
            .cluster("local")
            .labels(Map.of())
            .creationTimestamp(Date.from(Instant.now()))
            .generation(0)
            .build();

        assertEquals(original, same);
        assertNotEquals(original, different);
    }
}
