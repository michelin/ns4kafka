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

import java.util.List;
import java.util.stream.Stream;

/**
 * Holds the outcome of a resource validation pass.
 *
 * <p>Errors are hard failures that must block resource creation/update. Warnings are soft failures (e.g. lenient-mode
 * regex mismatches) that are surfaced to the caller but do not prevent the operation from succeeding.
 *
 * <p>The {@code soft} flag on {@link FieldValidationException} drives the routing: {@code soft=true} → warning,
 * {@code soft=false} → error.
 */
public record ValidationResult(List<String> errors, List<String> warnings) {

    /** Convenience factory for a result with no errors and no warnings. */
    public static ValidationResult empty() {
        return new ValidationResult(List.of(), List.of());
    }

    /**
     * Convenience factory for a result carrying only hard errors and no warnings.
     *
     * @param errors the list of hard errors
     * @return a ValidationResult with the given errors and an empty warnings list
     */
    public static ValidationResult ofErrors(List<String> errors) {
        return new ValidationResult(errors, List.of());
    }

    /**
     * Convenience factory for a result carrying only soft warnings and no hard errors.
     *
     * @param warnings the list of warnings
     * @return a ValidationResult with the given warnings and an empty errors list
     */
    public static ValidationResult ofWarnings(List<String> warnings) {
        return new ValidationResult(List.of(), warnings);
    }

    /**
     * Merge this result with another, concatenating both error and warning lists.
     *
     * @param other the other ValidationResult to merge
     * @return a new ValidationResult with the combined errors and warnings
     */
    public ValidationResult merge(ValidationResult other) {
        List<String> mergedErrors =
                Stream.concat(errors.stream(), other.errors().stream()).toList();
        List<String> mergedWarnings =
                Stream.concat(warnings.stream(), other.warnings().stream()).toList();
        return new ValidationResult(mergedErrors, mergedWarnings);
    }

    /** Returns {@code true} when there is at least one hard error. */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    /** Returns {@code true} when there is at least one soft warning. */
    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }
}
