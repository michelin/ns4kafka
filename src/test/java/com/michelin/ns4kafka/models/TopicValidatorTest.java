package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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

        TopicValidator differentByVal = TopicValidator.builder()
                .validationConstraints(
                        Map.of("replication.factor", ResourceValidator.Range.between(3, 99999999),
                                "partitions", ResourceValidator.Range.between(3, 6),
                                "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                                "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                                "retention.ms", ResourceValidator.Range.between(60000, 604800000)))
                .build();

        TopicValidator differentBySize = TopicValidator.builder()
                .validationConstraints(
                        Map.of("replication.factor", ResourceValidator.Range.between(3, 3),
                                "partitions", ResourceValidator.Range.between(3, 6),
                                "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                                "min.insync.replicas", ResourceValidator.Range.between(2, 2)))
                .build();

        assertEquals(original, same);
        assertEquals(original, sameReordered);

        assertNotEquals(original, differentByKey);
        assertNotEquals(original, differentByVal);
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
                .metadata(ObjectMeta.builder().name("valid_name").build())
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
                .metadata(ObjectMeta.builder().name("").build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(2, validationErrors.size());
        assertLinesMatch(
                List.of(".*Value must not be empty.*",".*Value must only contain.*"),
                validationErrors);

        invalidTopic = Topic.builder()
                .metadata(ObjectMeta.builder().name(".").build())
                .spec(Topic.TopicSpec.builder().build()).build();
        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());

        invalidTopic = Topic.builder()
                .metadata(ObjectMeta.builder().name("..").build())
                .spec(Topic.TopicSpec.builder().build()).build();
        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());

        invalidTopic = Topic.builder()
                .metadata(ObjectMeta.builder().name("A".repeat(260)).build())
                .spec(Topic.TopicSpec.builder().build()).build();
        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());

        invalidTopic = Topic.builder()
                .metadata(ObjectMeta.builder().name("A B").build())
                .spec(Topic.TopicSpec.builder().build()).build();
        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());

        invalidTopic = Topic.builder()
                .metadata(ObjectMeta.builder().name("topicname<invalid").build())
                .spec(Topic.TopicSpec.builder().build()).build();

        validationErrors = nameValidator.validate(invalidTopic);
        assertEquals(1, validationErrors.size());
    }

    @Test
    void shouldValidateWithNoValidationConstraint() {
        TopicValidator topicValidator = TopicValidator.builder()
                .build();

        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("validName").build())
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
                .metadata(ObjectMeta.builder().name("validName").build())
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
                .metadata(ObjectMeta.builder().name("validName").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .build())
                .build();

        List<String> actual = topicValidator.validate(topic);
        assertEquals(3, actual.size());
        assertTrue(actual.contains("Invalid value null for configuration min.insync.replicas: Value must be non-null"));
        assertTrue(actual.contains("Invalid value null for configuration retention.ms: Value must be non-null"));
        assertTrue(actual.contains("Invalid value null for configuration cleanup.policy: Value must be non-null"));
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
                .metadata(ObjectMeta.builder().name("validName").build())
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
        assertEquals("Configurations [retention.bytes] are not allowed", actual.get(0));
    }
}
