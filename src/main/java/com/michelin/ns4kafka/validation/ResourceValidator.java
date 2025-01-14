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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidFieldValidationAtLeast;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidFieldValidationAtMost;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidFieldValidationEmpty;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidFieldValidationNull;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidFieldValidationNumber;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidFieldValidationOneOf;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Resource validator.
 */
@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public abstract class ResourceValidator {
    @Builder.Default
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    protected Map<String, Validator> validationConstraints = new HashMap<>();

    /**
     * Validate the configuration.
     */
    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "validation-type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Range.class, name = "Range"),
        @JsonSubTypes.Type(value = ValidList.class, name = "ValidList"),
        @JsonSubTypes.Type(value = ValidString.class, name = "ValidString"),
        @JsonSubTypes.Type(value = NonEmptyString.class, name = "NonEmptyString"),
        @JsonSubTypes.Type(value = CompositeValidator.class, name = "CompositeValidator")
    })
    public interface Validator {
        /**
         * Perform single configuration validation.
         *
         * @param name  The name of the configuration
         * @param value The value of the configuration
         * @throws FieldValidationException if the value is invalid.
         */
        void ensureValid(String name, Object value);
    }

    /**
     * Validation logic for numeric ranges.
     */
    @Data
    @NoArgsConstructor
    public static class Range implements ResourceValidator.Validator {
        private Number min;
        private Number max;
        private boolean optional = false;

        /**
         * A numeric range with inclusive upper bound and inclusive lower bound.
         *
         * @param min the lower bound
         * @param max the upper bound
         */
        public Range(Number min, Number max, boolean optional) {
            this.min = min;
            this.max = max;
            this.optional = optional;
        }

        /**
         * A numeric range that checks only the lower bound.
         *
         * @param min The minimum acceptable value
         */
        public static ResourceValidator.Range atLeast(Number min) {
            return new ResourceValidator.Range(min, null, false);
        }

        /**
         * A numeric range that checks both the upper (inclusive) and lower bound.
         */
        public static ResourceValidator.Range between(Number min, Number max) {
            return new ResourceValidator.Range(min, max, false);
        }

        /**
         * A numeric range that checks both the upper (inclusive) and lower bound, and accepts null as well.
         */
        public static ResourceValidator.Range optionalBetween(Number min, Number max) {
            return new ResourceValidator.Range(min, max, true);
        }

        /**
         * Ensure that the value is in the range.
         *
         * @param name The name of the configuration
         * @param o    The value of the configuration
         */
        public void ensureValid(String name, Object o) {
            Number n;
            if (o == null) {
                if (optional) {
                    return;
                }
                throw new FieldValidationException(invalidFieldValidationNull(name));
            }
            try {
                n = Double.valueOf(o.toString());
            } catch (NumberFormatException e) {
                throw new FieldValidationException(invalidFieldValidationNumber(name, o.toString()));
            }
            if (min != null && n.doubleValue() < min.doubleValue()) {
                throw new FieldValidationException(invalidFieldValidationAtLeast(name, o.toString(), min));
            }
            if (max != null && n.doubleValue() > max.doubleValue()) {
                throw new FieldValidationException(invalidFieldValidationAtMost(name, o.toString(), max));
            }
        }

        /**
         * Return a string representation of the range.
         *
         * @return A string representation of the range
         */
        public String toString() {
            if (min == null && max == null) {
                return "[...]";
            } else if (min == null) {
                return "[...," + max + "]";
            } else if (max == null) {
                return "[" + min + ",...]";
            } else {
                return "[" + min + ",...," + max + "]";
            }
        }
    }

    /**
     * Validation logic for a list of valid strings.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidList implements ResourceValidator.Validator {
        private List<String> validStrings;
        private boolean optional = false;

        public static ResourceValidator.ValidList in(String... validStrings) {
            return new ResourceValidator.ValidList(Arrays.asList(validStrings), false);
        }

        public static ResourceValidator.ValidList optionalIn(String... validStrings) {
            return new ResourceValidator.ValidList(Arrays.asList(validStrings), true);
        }

        /**
         * Ensure that the value is one of the valid strings.
         *
         * @param name The name of the configuration
         * @param o    The value of the configuration
         */
        @Override
        public void ensureValid(final String name, final Object o) {
            if (o == null) {
                if (optional) {
                    return;
                }
                throw new FieldValidationException(invalidFieldValidationNull(name));
            }
            String s = (String) o;

            List<String> values = List.of(s); //default if no "," (most of the time)
            if (s.contains(",")) {
                //split and strip
                values = Arrays.stream(s.split(",")).map(String::strip).toList();
            }

            for (String string : values) {
                ValidString validString = new ValidString(validStrings, false);
                validString.ensureValid(name, string);
            }
        }

        /**
         * Return a string representation of the valid strings.
         *
         * @return A string representation of the valid strings
         */
        public String toString() {
            return validStrings.toString();
        }
    }

    /**
     * Validation logic for a valid string.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidString implements ResourceValidator.Validator {
        private List<String> validStrings;
        private boolean optional = false;

        public static ResourceValidator.ValidString in(String... validStrings) {
            return new ResourceValidator.ValidString(Arrays.asList(validStrings), false);
        }

        public static ResourceValidator.ValidString optionalIn(String... validStrings) {
            return new ResourceValidator.ValidString(Arrays.asList(validStrings), true);
        }

        /**
         * Ensure that the value is one of the valid strings.
         *
         * @param name The name of the configuration
         * @param o    The value of the configuration
         */
        @Override
        public void ensureValid(String name, Object o) {
            if (o == null) {
                if (optional) {
                    return;
                }
                throw new FieldValidationException(invalidFieldValidationNull(name));
            }
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new FieldValidationException(invalidFieldValidationOneOf(name, o.toString(),
                    String.join(",", validStrings)));
            }

        }

        /**
         * Return a string representation of the valid strings.
         *
         * @return A string representation of the valid strings
         */
        public String toString() {
            return "[" + String.join(", ", validStrings) + "]";
        }
    }

    /**
     * Validation logic for a non-empty string.
     */
    public static class NonEmptyString implements ResourceValidator.Validator {
        /**
         * Ensure that the value is non-empty.
         *
         * @param name The name of the configuration
         * @param o    The value of the configuration
         */
        @Override
        public void ensureValid(String name, Object o) {
            if (o == null) {
                throw new FieldValidationException(invalidFieldValidationNull(name));
            }
            String s = (String) o;
            if (s.isEmpty()) {
                throw new FieldValidationException(invalidFieldValidationEmpty(name));
            }
        }

        /**
         * Return a string representation of the valid strings.
         *
         * @return A string representation of the valid strings
         */
        @Override
        public String toString() {
            return "non-empty string";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            return obj instanceof NonEmptyString;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

    /**
     * Validation logic for a composite validator.
     */
    @Data
    @NoArgsConstructor
    public static class CompositeValidator implements ResourceValidator.Validator {
        private List<ResourceValidator.Validator> validators;

        private CompositeValidator(List<ResourceValidator.Validator> validators) {
            this.validators = Collections.unmodifiableList(validators);
        }

        public static ResourceValidator.CompositeValidator of(ResourceValidator.Validator... validators) {
            return new ResourceValidator.CompositeValidator(Arrays.asList(validators));
        }

        @Override
        public void ensureValid(String name, Object value) {
            for (ResourceValidator.Validator validator : validators) {
                validator.ensureValid(name, value);
            }
        }

        @Override
        public String toString() {
            if (validators == null) {
                return EMPTY_STRING;
            }
            StringBuilder desc = new StringBuilder();
            for (ResourceValidator.Validator v : validators) {
                if (!desc.isEmpty()) {
                    desc.append(',').append(' ');
                }
                desc.append(v);
            }
            return desc.toString();
        }
    }
}
