package com.michelin.ns4kafka.validation;

import static com.michelin.ns4kafka.utils.config.TopicConfig.PARTITIONS;
import static com.michelin.ns4kafka.utils.config.TopicConfig.REPLICATION_FACTOR;

import com.michelin.ns4kafka.models.Topic;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * Topic validator.
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TopicValidator extends ResourceValidator {
    /**
     * Build a default topic validator.
     *
     * @return The topic validator
     */
    public static TopicValidator makeDefault() {
        return TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(3, 3),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(2, 2),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000),
                    "retention.bytes", ResourceValidator.Range.optionalBetween(-1, 104857600),
                    "preallocate", ResourceValidator.ValidString.optionalIn("true", "false")
                )
            )
            .build();
    }

    /**
     * Build a default topic validator for one broker.
     *
     * @return The topic validator
     */
    public static TopicValidator makeDefaultOneBroker() {
        return TopicValidator.builder()
            .validationConstraints(
                Map.of("replication.factor", ResourceValidator.Range.between(1, 1),
                    "partitions", ResourceValidator.Range.between(3, 6),
                    "cleanup.policy", ResourceValidator.ValidList.in("delete", "compact"),
                    "min.insync.replicas", ResourceValidator.Range.between(1, 1),
                    "retention.ms", ResourceValidator.Range.between(60000, 604800000),
                    "retention.bytes", ResourceValidator.Range.optionalBetween(-1, 104857600),
                    "preallocate", ResourceValidator.ValidString.optionalIn("true", "false")
                )
            )
            .build();
    }

    /**
     * Validate a given topic.
     *
     * @param topic The topic
     * @return A list of validation errors
     * @see <a href="https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L36">GitHub</a>
     */
    public List<String> validate(Topic topic) {
        List<String> validationErrors = new ArrayList<>();

        if (topic.getMetadata().getName().isEmpty()) {
            validationErrors.add(
                "Invalid value " + topic.getMetadata().getName() + " for name: Value must not be empty");
        }

        if (topic.getMetadata().getName().equals(".") || topic.getMetadata().getName().equals("..")) {
            validationErrors.add(
                "Invalid value " + topic.getMetadata().getName() + " for name: Value must not be \".\" or \"..\"");
        }

        if (topic.getMetadata().getName().length() > 249) {
            validationErrors.add(
                "Invalid value " + topic.getMetadata().getName() + " for name: Value must not be longer than 249");
        }

        if (!topic.getMetadata().getName().matches("[a-zA-Z0-9._-]+")) {
            validationErrors.add(
                "Invalid value " + topic.getMetadata().getName() + " for name: Value must only contain "
                    + "ASCII alphanumerics, '.', '_' or '-'");
        }

        if (!validationConstraints.isEmpty() && topic.getSpec().getConfigs() != null) {
            Set<String> configsWithoutConstraints = topic.getSpec().getConfigs().keySet()
                .stream()
                .filter(s -> !validationConstraints.containsKey(s))
                .collect(Collectors.toSet());
            if (!configsWithoutConstraints.isEmpty()) {
                validationErrors.add(
                    "Configurations [" + String.join(",", configsWithoutConstraints) + "] are not allowed");
            }
        }

        validationConstraints.forEach((key, value) -> {
            try {
                if (key.equals(PARTITIONS)) {
                    value.ensureValid(key, topic.getSpec().getPartitions());
                } else if (key.equals(REPLICATION_FACTOR)) {
                    value.ensureValid(key, topic.getSpec().getReplicationFactor());
                } else {
                    if (topic.getSpec().getConfigs() != null) {
                        value.ensureValid(key, topic.getSpec().getConfigs().get(key));
                    } else {
                        validationErrors.add(
                            "Invalid value null for configuration " + key + ": Value must be non-null");
                    }
                }
            } catch (FieldValidationException e) {
                validationErrors.add(e.getMessage());
            }
        });
        return validationErrors;
    }

}
