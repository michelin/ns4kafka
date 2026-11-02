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

import com.michelin.ns4kafka.model.TableflowTopic;
import io.micronaut.serde.annotation.Serdeable;
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

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
        List<String> validationErrors = new ArrayList<>();
        List<String> validationWarnings = new ArrayList<>();

        validationConstraints.forEach((key, value) -> {
            try {
                String fieldValue =
                        switch (key) {
                            case "name" -> tableflowTopic.getMetadata().getName();
                            case "table.format" ->
                                tableflowTopic.getSpec().getTableFormats() == null
                                        ? null
                                        : String.join(
                                                ",", tableflowTopic.getSpec().getTableFormats());
                            case "storage.kind" ->
                                tableflowTopic.getSpec().getStorage() == null
                                        ? null
                                        : tableflowTopic.getSpec().getStorage().getKind();
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
