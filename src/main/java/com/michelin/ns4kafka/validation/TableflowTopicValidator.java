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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidImmutableField;

import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.TableflowTopic.TableflowTopicSpec;
import io.micronaut.serde.annotation.Serdeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.jspecify.annotations.Nullable;

/** Tableflow topic validator. */
@Getter
@Setter
@Serdeable
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TableflowTopicValidator extends ResourceValidator {
    /**
     * Validate a Tableflow topic against the namespace constraints.
     *
     * @param tableflowTopic The Tableflow topic
     * @return The validation errors and warnings
     */
    public ValidationResult validate(TableflowTopic tableflowTopic) {
        return validate(tableflowTopic, null);
    }

    /**
     * Validate immutable fields and apply namespace rules to the effective configuration.
     *
     * @param tableflowTopic The requested Tableflow topic
     * @param existingSpec The stored spec, or null on creation
     * @return The validation errors and warnings
     */
    public ValidationResult validate(TableflowTopic tableflowTopic, @Nullable TableflowTopicSpec existingSpec) {
        TableflowTopicSpec spec = tableflowTopic.getSpec();
        List<String> validationErrors = new ArrayList<>();
        if (existingSpec != null) {
            if (!Objects.equals(existingSpec.getTableFormats(), spec.getTableFormats())) {
                validationErrors.add(invalidImmutableField("tableFormats"));
            }
            if (!Objects.equals(existingSpec.getStorage(), spec.getStorage())) {
                validationErrors.add(invalidImmutableField("storage.kind"));
            }
        }
        Long retention = spec.getSnapshotRetentionMs() == null && existingSpec != null
                ? existingSpec.getSnapshotRetentionMs()
                : spec.getSnapshotRetentionMs();
        List<String> validationWarnings = new ArrayList<>();

        validationConstraints.forEach((key, value) -> {
            try {
                String fieldValue =
                        switch (key) {
                            case "name" -> tableflowTopic.getMetadata().getName();
                            case "tableFormats" ->
                                spec.getTableFormats() == null ? null : String.join(",", spec.getTableFormats());
                            case "storage.kind" ->
                                spec.getStorage() == null
                                        ? null
                                        : spec.getStorage().getKind();
                            case "snapshotRetentionMs" -> Objects.toString(retention, null);
                            default -> null;
                        };
                value.ensureValid(key, fieldValue);
            } catch (FieldValidationException e) {
                if (e.soft) {
                    validationWarnings.add(e.getMessage());
                } else {
                    validationErrors.add(e.getMessage());
                }
            }
        });

        return new ValidationResult(validationErrors, validationWarnings);
    }
}
