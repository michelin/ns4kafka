package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TopicValidatorTest {
    @Test
    void testEquals() {
        TopicValidator original = TopicValidator.builder()
                .validationConstraints(
                        Map.of( "replication.factor", ResourceValidator.Range.between(3,3),
                                "partitions", ResourceValidator.Range.between(3,6),
                                "cleanup.policy", ResourceValidator.ValidList.in("delete","compact"),
                                "min.insync.replicas", ResourceValidator.Range.between(2,2),
                                "retention.ms", ResourceValidator.Range.between(60000,604800000)
                        )
                )
                .build();
        TopicValidator same = TopicValidator.builder()
                .validationConstraints(
                        Map.of( "replication.factor", ResourceValidator.Range.between(3,3),
                                "partitions", ResourceValidator.Range.between(3,6),
                                "cleanup.policy", ResourceValidator.ValidList.in("delete","compact"),
                                "min.insync.replicas", ResourceValidator.Range.between(2,2),
                                "retention.ms", ResourceValidator.Range.between(60000,604800000)
                        )
                )
                .build();
        TopicValidator sameReordered = TopicValidator.builder()
                .validationConstraints(
                        Map.of("partitions", ResourceValidator.Range.between(3,6),
                                "cleanup.policy", ResourceValidator.ValidList.in("delete","compact"),
                                "min.insync.replicas", ResourceValidator.Range.between(2,2),
                                // move from position 1
                                "replication.factor", ResourceValidator.Range.between(3,3),
                                "retention.ms", ResourceValidator.Range.between(60000,604800000)
                        )
                )
                .build();
        TopicValidator differentByKey = TopicValidator.builder()
                .validationConstraints(
                        Map.of( "DIFFERENT_replication.factor", ResourceValidator.Range.between(3,3),
                                "partitions", ResourceValidator.Range.between(3,6),
                                "cleanup.policy", ResourceValidator.ValidList.in("delete","compact"),
                                "min.insync.replicas", ResourceValidator.Range.between(2,2),
                                "retention.ms", ResourceValidator.Range.between(60000,604800000)
                        )
                )
                .build();
        TopicValidator differentByVal = TopicValidator.builder()
                .validationConstraints(
                        Map.of( "replication.factor", ResourceValidator.Range.between(3,99999999),
                                "partitions", ResourceValidator.Range.between(3,6),
                                "cleanup.policy", ResourceValidator.ValidList.in("delete","compact"),
                                "min.insync.replicas", ResourceValidator.Range.between(2,2),
                                "retention.ms", ResourceValidator.Range.between(60000,604800000)
                        )
                )
                .build();
        TopicValidator differentBySize = TopicValidator.builder()
                .validationConstraints(
                        Map.of( "replication.factor", ResourceValidator.Range.between(3,3),
                                "partitions", ResourceValidator.Range.between(3,6),
                                "cleanup.policy", ResourceValidator.ValidList.in("delete","compact"),
                                "min.insync.replicas", ResourceValidator.Range.between(2,2)
                        )
                )
                .build();
        Assertions.assertEquals(original, same);
        Assertions.assertEquals(original, sameReordered);

        Assertions.assertNotEquals(original, differentByKey);
        Assertions.assertNotEquals(original, differentByVal);
        Assertions.assertNotEquals(original, differentBySize);


    }
}
