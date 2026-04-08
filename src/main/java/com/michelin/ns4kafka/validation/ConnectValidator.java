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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNameEmpty;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNameLength;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNameSpecChars;
import static com.michelin.ns4kafka.util.config.ConnectorConfig.CONNECTOR_CLASS;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.michelin.ns4kafka.model.connect.Connector;
import io.micronaut.core.util.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/** Validator for connectors. */
@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ConnectValidator extends ResourceValidator {
    @Builder.Default
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    private Map<String, Validator> sourceValidationConstraints = new HashMap<>();

    @Builder.Default
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    private Map<String, Validator> sinkValidationConstraints = new HashMap<>();

    @Builder.Default
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    private Map<String, Map<String, Validator>> classValidationConstraints = new HashMap<>();

    /**
     * Make a default ConnectValidator.
     *
     * @return A ConnectValidator
     */
    public static ConnectValidator makeDefault() {
        return ConnectValidator.builder()
                .validationConstraints(Map.of(
                        "key.converter",
                        new ResourceValidator.NonEmptyString(),
                        "value.converter",
                        new ResourceValidator.NonEmptyString(),
                        CONNECTOR_CLASS,
                        new ResourceValidator.ValidString(
                                List.of(
                                        "io.confluent.connect.jdbc.JdbcSourceConnector",
                                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                                        "com.splunk.kafka.connect.SplunkSinkConnector",
                                        "org.apache.kafka.connect.file.FileStreamSinkConnector"),
                                false)))
                .sourceValidationConstraints(
                        Map.of("producer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .sinkValidationConstraints(
                        Map.of("consumer.override.sasl.jaas.config", new ResourceValidator.NonEmptyString()))
                .classValidationConstraints(Map.of(
                        "io.confluent.connect.jdbc.JdbcSinkConnector",
                        Map.of("db.timezone", new ResourceValidator.NonEmptyString())))
                .build();
    }

    /**
     * Validate a given connector.
     *
     * <p>Hard failures (soft=false) are returned as errors; lenient-mode regex mismatches (soft=true) are returned as
     * warnings so the resource can still be created while the caller is notified.
     *
     * @param connector The connector
     * @param connectorType The connector type
     * @return A {@link ValidationResult} containing errors and warnings
     */
    public ValidationResult validate(Connector connector, String connectorType) {
        List<String> validationErrors = new ArrayList<>();
        List<String> validationWarnings = new ArrayList<>();

        ValidationResult nameValidation = validateName(connector.getMetadata().getName());
        if (nameValidation != null) {
            // Hard error in name (blank / too long / special chars) → stop immediately.
            if (nameValidation.hasErrors()) {
                return nameValidation;
            }
            // Soft warning from name constraint → accumulate and continue.
            validationWarnings.addAll(nameValidation.warnings());
        }

        applyConstraints(validationConstraints, connector, validationErrors, validationWarnings);
        applyConstraints(getConnectorTypeConstraints(connectorType), connector, validationErrors, validationWarnings);
        applyConstraints(getClassConstraints(connector), connector, validationErrors, validationWarnings);

        return new ValidationResult(validationErrors, validationWarnings);
    }

    /**
     * Validates the name field of a connector's metadata.
     *
     * <p>Checks are applied in two phases:
     *
     * <ol>
     *   <li>Blank check — returns immediately with a single hard error if the name is empty/null.
     *   <li>Structural checks (length &gt; 249, illegal characters) — both are evaluated and returned together if
     *       either fails, so the caller sees all structural problems at once.
     *   <li>Constraint check — only evaluated when structural checks pass; honours the
     *       {@link FieldValidationException#soft soft} flag to route to warnings or errors.
     * </ol>
     *
     * @param name The connector name from {@code connector.getMetadata().getName()}
     * @return A {@link ValidationResult} wrapping any name-level failures, or {@code null} if the name is valid
     */
    private ValidationResult validateName(String name) {
        if (!StringUtils.hasText(name)) {
            return ValidationResult.ofErrors(List.of(invalidNameEmpty()));
        }

        // Collect structural errors together so the caller sees all of them at once.
        List<String> structuralErrors = new ArrayList<>();
        if (name.length() > 249) {
            structuralErrors.add(invalidNameLength(name));
        }
        if (!name.matches("[a-zA-Z0-9._-]+")) {
            structuralErrors.add(invalidNameSpecChars(name));
        }
        if (!structuralErrors.isEmpty()) {
            return ValidationResult.ofErrors(structuralErrors);
        }

        // Constraint-based name check — only reached when structural checks pass.
        if (validationConstraints.containsKey("name")) {
            try {
                validationConstraints.get("name").ensureValid("name", name);
            } catch (FieldValidationException e) {
                if (e.soft) {
                    return ValidationResult.ofWarnings(List.of(e.getMessage()));
                }
                return ValidationResult.ofErrors(List.of(e.getMessage()));
            }
        }
        return null;
    }

    /**
     * Resolves the set of validation constraints that apply to a given connector type.
     *
     * <p>Returns {@link #sinkValidationConstraints} for {@code "sink"}, {@link #sourceValidationConstraints} for
     * {@code "source"}, and an empty map for any unrecognized type — allowing {@link #applyConstraints} to be called
     * unconditionally at the call site.
     *
     * @param connectorType The connector type (e.g. {@code "sink"} or {@code "source"})
     * @return The corresponding constraint map, or an empty map if the type is unrecognized
     */
    private Map<String, Validator> getConnectorTypeConstraints(String connectorType) {
        return switch (connectorType) {
            case "sink" -> sinkValidationConstraints;
            case "source" -> sourceValidationConstraints;
            default -> Map.of();
        };
    }

    /**
     * Resolves the set of class-specific validation constraints for the given connector.
     *
     * <p>Looks up the connector's class name (from {@code connector.getSpec().getConfig().get(CONNECTOR_CLASS)}) in
     * {@link #classValidationConstraints}. Returns an empty map if no class-specific constraints are registered,
     * allowing {@link #applyConstraints} to be called unconditionally at the call site.
     *
     * @param connector The connector whose class-specific constraints should be resolved
     * @return The constraint map for the connector's class, or an empty map if none are registered
     */
    private Map<String, Validator> getClassConstraints(Connector connector) {
        String connectorClass = connector.getSpec().getConfig().get(CONNECTOR_CLASS);
        return classValidationConstraints.getOrDefault(connectorClass, Map.of());
    }

    /**
     * Applies a set of validation constraints against a connector's config, routing each outcome to the appropriate
     * list.
     *
     * <p>For each entry in {@code constraints}, calls {@link Validator#ensureValid(String, Object)} with the
     * corresponding value from the connector's config. Any {@link FieldValidationException} is routed to
     * {@code warnings} if {@link FieldValidationException#soft soft} is {@code true}, or to {@code errors} otherwise.
     *
     * @param constraints The map of config key to {@link Validator} to apply
     * @param connector The connector whose config values are being validated
     * @param errors Accumulator for hard validation failures
     * @param warnings Accumulator for soft validation failures
     */
    private void applyConstraints(
            Map<String, Validator> constraints, Connector connector, List<String> errors, List<String> warnings) {
        constraints.forEach((key, validator) -> {
            try {
                validator.ensureValid(key, connector.getSpec().getConfig().get(key));
            } catch (FieldValidationException e) {
                (e.soft ? warnings : errors).add(e.getMessage());
            }
        });
    }
}
