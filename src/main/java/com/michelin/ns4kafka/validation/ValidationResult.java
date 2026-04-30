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

/** Holds the outcome of a resource validation pass. */
public record ValidationResult(List<String> errors, List<String> warnings) {

    /**
     * Build an empty result.
     *
     * @return A result with no errors and no warnings
     */
    public static ValidationResult empty() {
        return new ValidationResult(List.of(), List.of());
    }

    /**
     * Build a result with only errors.
     *
     * @param errors The hard errors
     * @return A result with the given errors and no warnings
     */
    public static ValidationResult ofErrors(List<String> errors) {
        return new ValidationResult(errors, List.of());
    }

    /**
     * Check if the result has at least one hard error.
     *
     * @return {@code true} if there is at least one hard error
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}
