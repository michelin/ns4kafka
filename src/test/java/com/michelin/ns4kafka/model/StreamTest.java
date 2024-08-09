package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class StreamTest {
    @Test
    void shouldBeEqual() {
        KafkaStream original = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("stream1")
                .build())
            .build();

        KafkaStream same = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("stream1")
                .build())
            .build();

        KafkaStream different = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("stream2")
                .build())
            .build();

        assertEquals(original, same);
        assertNotEquals(original, different);
    }
}
