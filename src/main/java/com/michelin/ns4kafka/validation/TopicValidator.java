package com.michelin.ns4kafka.validation;

import static com.michelin.ns4kafka.utils.config.TopicConfig.PARTITIONS;
import static com.michelin.ns4kafka.utils.config.TopicConfig.REPLICATION_FACTOR;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidFieldValidationNull;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidNameEmpty;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidNameLength;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidNameSpecChars;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidTopicName;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidTopicSpec;

import com.michelin.ns4kafka.models.Topic;
import io.micronaut.core.util.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

        if (!StringUtils.hasText(topic.getMetadata().getName())) {
            validationErrors.add(invalidNameEmpty());
        }

        if (topic.getMetadata().getName().equals(".") || topic.getMetadata().getName().equals("..")) {
            validationErrors.add(invalidTopicName(topic.getMetadata().getName()));
        }

        if (topic.getMetadata().getName().length() > 249) {
            validationErrors.add(invalidNameLength(topic.getMetadata().getName()));
        }

        if (!topic.getMetadata().getName().matches("[a-zA-Z0-9._-]+")) {
            validationErrors.add(invalidNameSpecChars(topic.getMetadata().getName()));
        }

        if (!validationConstraints.isEmpty() && topic.getSpec().getConfigs() != null) {
            Map<String, String> configsWithoutConstraints = topic.getSpec().getConfigs().entrySet()
                .stream()
                .filter(entry -> !validationConstraints.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!configsWithoutConstraints.isEmpty()) {
                configsWithoutConstraints
                    .forEach((key, value) -> validationErrors.add(invalidTopicSpec(key, value)));
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
                        validationErrors.add(invalidFieldValidationNull(key));
                    }
                }
            } catch (FieldValidationException e) {
                validationErrors.add(e.getMessage());
            }
        });
        return validationErrors;
    }

}
