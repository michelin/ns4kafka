/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.validation;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidFieldValidationNull;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNameEmpty;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNameLength;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNameSpecChars;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTopicName;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTopicSpec;
import static com.michelin.ns4kafka.util.config.TopicConfig.PARTITIONS;
import static com.michelin.ns4kafka.util.config.TopicConfig.REPLICATION_FACTOR;

import com.michelin.ns4kafka.model.Topic;
import io.micronaut.core.util.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/** Topic validator. */
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
                .validationConstraints(Map.of(
                        REPLICATION_FACTOR,
                        ResourceValidator.Range.between(3, 3),
                        PARTITIONS,
                        ResourceValidator.Range.between(3, 6),
                        "cleanup.policy",
                        ResourceValidator.ValidList.in("delete", "compact"),
                        "min.insync.replicas",
                        ResourceValidator.Range.between(2, 2),
                        "retention.ms",
                        ResourceValidator.Range.between(60000, 604800000),
                        "retention.bytes",
                        ResourceValidator.Range.optionalBetween(-1, 104857600),
                        "preallocate",
                        ResourceValidator.ValidString.optionalIn("true", "false")))
                .build();
    }

    /**
     * Build a default topic validator for one broker.
     *
     * @return The topic validator
     */
    public static TopicValidator makeDefaultOneBroker() {
        return TopicValidator.builder()
                .validationConstraints(Map.of(
                        REPLICATION_FACTOR,
                        ResourceValidator.Range.between(1, 1),
                        PARTITIONS,
                        ResourceValidator.Range.between(3, 6),
                        "cleanup.policy",
                        ResourceValidator.ValidList.in("delete", "compact"),
                        "min.insync.replicas",
                        ResourceValidator.Range.between(1, 1),
                        "retention.ms",
                        ResourceValidator.Range.between(60000, 604800000),
                        "retention.bytes",
                        ResourceValidator.Range.optionalBetween(-1, 104857600),
                        "preallocate",
                        ResourceValidator.ValidString.optionalIn("true", "false")))
                .build();
    }

    public ValidationResult validate(Topic topic) {
        List<String> validationErrors = new ArrayList<>();
        List<String> validationWarnings = new ArrayList<>();

        ValidationResult nameResult = validateName(topic.getMetadata().getName());
        validationErrors.addAll(nameResult.errors());
        validationWarnings.addAll(nameResult.warnings());

        ValidationResult unknownConfigsResult = validateUnknownConfigs(topic);
        validationErrors.addAll(unknownConfigsResult.errors());

        ValidationResult constraintsResult = applyConstraints(validationConstraints, topic);
        validationErrors.addAll(constraintsResult.errors());
        validationWarnings.addAll(constraintsResult.warnings());

        return new ValidationResult(validationErrors, validationWarnings);
    }

    /**
     * Validates the name field of a topic's metadata.
     *
     * <p>Unlike connector name validation, all structural checks are independent and accumulate — the caller sees every
     * problem at once. The blank check is the only short-circuit: if the name is empty or null, further checks would be
     * meaningless and are skipped.
     *
     * <p>Checks applied :
     *
     * <ol>
     *   <li>Name must not be {@code "."} or {@code ".."}
     *   <li>Name must not exceed 249 characters
     *   <li>Name must only contain {@code [a-zA-Z0-9._-]}
     * </ol>
     *
     * @param name The topic name from {@code topic.getMetadata().getName()}
     * @return A {@link ValidationResult} containing any name-level errors, empty if the name is valid
     */
    private ValidationResult validateName(String name) {
        List<String> errors = new ArrayList<>();

        if (!StringUtils.hasText(name)) {
            errors.add(invalidNameEmpty());
        }
        if (List.of(".", "..").contains(name)) {
            errors.add(invalidTopicName(name));
        }
        if (name != null && name.length() > 249) {
            errors.add(invalidNameLength(name));
        }
        if (name != null && !name.matches("[a-zA-Z0-9._-]+")) {
            errors.add(invalidNameSpecChars(name));
        }

        return ValidationResult.ofErrors(errors);
    }

    /**
     * Checks that every key in the topic's config is covered by a known validation constraint.
     *
     * <p>Any config entry whose key is not present in {@link #validationConstraints} is considered an
     * unknown/unsupported field and produces a hard error. The check is skipped entirely when
     * {@link #validationConstraints} is empty or the topic's config map is {@code null}.
     *
     * @param topic The topic whose config is being inspected
     * @return A {@link ValidationResult} containing any unknown-config errors
     */
    private ValidationResult validateUnknownConfigs(Topic topic) {
        if (validationConstraints.isEmpty() || topic.getSpec().getConfigs() == null) {
            return ValidationResult.empty();
        }

        List<String> errors = topic.getSpec().getConfigs().entrySet().stream()
                .filter(entry -> !validationConstraints.containsKey(entry.getKey()))
                .map(entry -> invalidTopicSpec(entry.getKey(), entry.getValue()))
                .toList();

        return ValidationResult.ofErrors(errors);
    }

    /**
     * Applies a set of validation constraints against a topic's spec, routing each outcome to the appropriate list.
     *
     * <p>The value passed to {@link Validator#ensureValid(String, Object)} depends on the key:
     *
     * <ul>
     *   <li>{@link com.michelin.ns4kafka.util.config.TopicConfig#PARTITIONS PARTITIONS} →
     *       {@code topic.getSpec().getPartitions()}
     *   <li>{@link com.michelin.ns4kafka.util.config.TopicConfig#REPLICATION_FACTOR REPLICATION_FACTOR} →
     *       {@code topic.getSpec().getReplicationFactor()}
     *   <li>Any other key → the matching entry in {@code topic.getSpec().getConfigs()}; if the config map is
     *       {@code null}, a hard error is added directly and {@code ensureValid} is not called.
     * </ul>
     *
     * <p>Any {@link FieldValidationException} is routed to warnings if {@link FieldValidationException#soft soft} is
     * {@code true}, or to errors otherwise.
     *
     * @param constraints The map of config key to {@link Validator} to apply
     * @param topic The topic whose spec values are being validated
     * @return A {@link ValidationResult} containing any constraint errors and warnings
     */
    private ValidationResult applyConstraints(Map<String, Validator> constraints, Topic topic) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        constraints.forEach((key, validator) -> {
            try {
                switch (key) {
                    case PARTITIONS ->
                        validator.ensureValid(key, topic.getSpec().getPartitions());
                    case REPLICATION_FACTOR ->
                        validator.ensureValid(key, topic.getSpec().getReplicationFactor());
                    default -> {
                        if (topic.getSpec().getConfigs() == null) {
                            errors.add(invalidFieldValidationNull(key));
                        } else {
                            validator.ensureValid(
                                    key, topic.getSpec().getConfigs().get(key));
                        }
                    }
                }
            } catch (FieldValidationException e) {
                (e.soft ? warnings : errors).add(e.getMessage());
            }
        });

        return new ValidationResult(errors, warnings);
    }
}
