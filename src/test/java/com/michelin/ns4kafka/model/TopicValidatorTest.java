package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TopicValidatorTest {
    @Test
    void testEquals() {
        TopicValidator original = TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(3, 3),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
            .build();

        TopicValidator same = TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(3, 3),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
            .build();

        TopicValidator sameReordered = TopicValidator.builder()
            .validationConstraints(
                Map.of("partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    // move from position 1
                    "replication.factor", ResourceValidator.Range.between(3, 3),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
            .build();

        TopicValidator differentByKey = TopicValidator.builder()
            .validationConstraints(
                Map.of("DIFFERENT_replication.factor", ResourceValidator.Range.between(3, 3),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
            .build();

        assertEquals(original, same);
        assertEquals(original, sameReordered);

        assertNotEquals(original, differentByKey);

        TopicValidator differentByVal = TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(3, 99999999),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
            .build();

        assertNotEquals(original, differentByVal);

        TopicValidator differentBySize = TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(3, 3),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2)))
            .build();

        assertNotEquals(original, differentBySize);
    }

    @Test
    void testEnsureValidGlobal() {
        TopicValidator topicValidator = TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(3, 3),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
            .build();

        Topic success = Topic.builder()
            .metadata(Metadata.builder().name("valid_name").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        List<String> actual = topicValidator.validate(success);
        assertTrue(actual.isEmpty());
    }

    @Test
    void testEnsureValidName() {
        TopicValidator nameValidator = TopicValidator.builder()
            .validationConstraints(Map.of())
            .build();

        Topic invalidTopic;
        List<String> validationErrors;

        invalidTopic = Topic.builder()
            .metadata(Metadata.builder().name("").build())
            .spec(Topic.TopicSpec.builder().build())
            .build();

        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(2, validationErrors.size());
        assertLinesMatch(
            List.of("Invalid empty value for field \"name\": value must not be empty.",
                "Invalid value \"\" for field \"name\": value must only contain ASCII alphanumerics, '.', '_' or '-'."),
            validationErrors);

        invalidTopic = Topic.builder()
            .metadata(Metadata.builder().name(".").build())
            .spec(Topic.TopicSpec.builder().build()).build();
        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());

        invalidTopic = Topic.builder()
            .metadata(Metadata.builder().name("..").build())
            .spec(Topic.TopicSpec.builder().build()).build();
        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());

        invalidTopic = Topic.builder()
            .metadata(Metadata.builder().name("A".repeat(260)).build())
            .spec(Topic.TopicSpec.builder().build()).build();
        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());

        invalidTopic = Topic.builder()
            .metadata(Metadata.builder().name("A B").build())
            .spec(Topic.TopicSpec.builder().build()).build();
        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());

        invalidTopic = Topic.builder()
            .metadata(Metadata.builder().name("topicname<invalid").build())
            .spec(Topic.TopicSpec.builder().build()).build();

        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());
    }

    @Test
    void shouldValidateWithNoValidationConstraint() {
        TopicValidator topicValidator = TopicValidator.builder()
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("validName").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000"))
                .build())
            .build();

        List<String> actual = topicValidator.validate(topic);
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldValidateWithNoValidationConstraintAndNoConfig() {
        TopicValidator topicValidator = TopicValidator.builder()
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("validName").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .build())
            .build();

        List<String> actual = topicValidator.validate(topic);
        assertTrue(actual.isEmpty());
    }

    @Test
    void shouldValidateWithNoConfig() {
        TopicValidator topicValidator = TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(3, 3),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("validName").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .build())
            .build();

        List<String> actual = topicValidator.validate(topic);
        assertEquals(3, actual.size());
        assertTrue(actual.contains("Invalid empty value for field \"min.insync.replicas\": value must not be null."));
        assertTrue(actual.contains("Invalid empty value for field \"retention.ms\": value must not be null."));
        assertTrue(actual.contains("Invalid empty value for field \"cleanup.policy\": value must not be null."));
    }

    @Test
    void shouldNotValidateBecauseConfigWithoutConstraint() {
        TopicValidator topicValidator = TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(3, 3),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
            .build();

        Topic topic = Topic.builder()
            .metadata(Metadata.builder().name("validName").build())
            .spec(Topic.TopicSpec.builder()
                .replicationFactor(3)
                .partitions(3)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "2",
                    "retention.ms", "60000",
                    "retention.bytes", "50"))
                .build())
            .build();

        List<String> actual = topicValidator.validate(topic);
        assertEquals(1, actual.size());
        assertEquals(
            "Invalid value \"50\" for field \"retention.bytes\": configuration is not allowed on your namespace.",
            actual.get(0));
    }
}
