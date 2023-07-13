package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class StreamTest {
    @Test
    void testEquals() {
        KafkaStream original = KafkaStream.builder()
                .metadata(ObjectMeta.builder().name("stream1").build())
                .build();

        KafkaStream same = KafkaStream.builder()
                .metadata(ObjectMeta.builder().name("stream1").build())
                .build();

        KafkaStream different = KafkaStream.builder()
                .metadata(ObjectMeta.builder().name("stream2").build())
                .build();

        assertEquals(original,same);
        assertNotEquals(original, different);
    }
}
