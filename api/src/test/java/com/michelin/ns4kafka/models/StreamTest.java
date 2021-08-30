package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamTest {
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

        Assertions.assertEquals(original,same);
        Assertions.assertNotEquals(original, different);
    }
}
