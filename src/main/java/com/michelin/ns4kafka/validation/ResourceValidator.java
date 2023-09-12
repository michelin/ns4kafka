package com.michelin.ns4kafka.validation;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.micronaut.serde.annotation.Serdeable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.*;

@Data
@Serdeable
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public abstract class ResourceValidator {
    @Builder.Default
    protected Map<String, Validator> validationConstraints = new HashMap<>();

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
         * @param name The name of the configuration
         * @param value The value of the configuration
         * @throws FieldValidationException if the value is invalid.
         */
        void ensureValid(String name, Object value);
    }

    /**
     * Validation logic for numeric ranges
     */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Range implements ResourceValidator.Validator {
        private Number min;
        private Number max;
        @Builder.Default
        private boolean optional = false;

        /**
         * A numeric range that checks only the lower bound
         *
         * @param min The minimum acceptable value
         */
        public static ResourceValidator.Range atLeast(Number min) {
            return new ResourceValidator.Range(min, null, false);
        }

        /**
         * A numeric range that checks both the upper (inclusive) and lower bound
         */
        public static ResourceValidator.Range between(Number min, Number max) {
            return new ResourceValidator.Range(min, max, false);
        }

        /**
         * A numeric range that checks both the upper (inclusive) and lower bound, and accepts null as well
         */
        public static ResourceValidator.Range optionalBetween(Number min, Number max) {
            return new ResourceValidator.Range(min, max, true);
        }

        @Override
        public void ensureValid(String name, Object o) {
            Number n;
            if (o == null) {
                if (optional)
                    return;
                throw new FieldValidationException(name, null, "Value must be non-null");
            }
            try {
                n = Double.valueOf(o.toString());
            }catch (NumberFormatException e){
                throw new FieldValidationException(name,o.toString(),"Value must be a Number");
            }
            if (min != null && n.doubleValue() < min.doubleValue())
                throw new FieldValidationException(name, o, "Value must be at least " + min);
            if (max != null && n.doubleValue() > max.doubleValue())
                throw new FieldValidationException(name, o, "Value must be no more than " + max);
        }

        public String toString() {
            if (min == null && max == null)
                return "[...]";
            else if (min == null)
                return "[...," + max + "]";
            else if (max == null)
                return "[" + min + ",...]";
            else
                return "[" + min + ",...," + max + "]";
        }
    }

    @Data
    @Serdeable
    @NoArgsConstructor
    public static class ValidList implements ResourceValidator.Validator {
        private List<String> validStrings;
        private boolean optional = false;

        public ValidList(List<String> validStrings, boolean optional) {
            this.validStrings = validStrings;
            this.optional = optional;
        }

        public static ResourceValidator.ValidList in(String... validStrings) {
            return new ResourceValidator.ValidList(Arrays.asList(validStrings), false);
        }
        public static ResourceValidator.ValidList optionalIn(String... validStrings) {
            return new ResourceValidator.ValidList(Arrays.asList(validStrings), true);
        }

        @Override
        public void ensureValid(final String name, final Object o) {
            if (o == null) {
                if (optional)
                    return;
                throw new FieldValidationException(name, null, "Value must be non-null");
            }
            String s = (String)o;

            List<String> values = List.of(s); //default if no "," (most of the time)
            if(s.contains(",")){
                //split and strip
                values = Arrays.stream(s.split(",")).map(String::strip).toList();
            }

            for (String string : values) {
                ValidString validString = new ValidString(validStrings, false);
                validString.ensureValid(name, string);
            }
        }

        public String toString() {
            return validStrings.toString();
        }
    }

    @Data
    @Serdeable
    @NoArgsConstructor
    public static class ValidString implements ResourceValidator.Validator {
        private List<String> validStrings;
        private boolean optional = false;

        public ValidString(List<String> validStrings, boolean optional) {
            this.validStrings = validStrings;
            this.optional = optional;
        }

        public static ResourceValidator.ValidString in(String... validStrings) {
            return new ResourceValidator.ValidString(Arrays.asList(validStrings), false);
        }

        public static ResourceValidator.ValidString optionalIn(String... validStrings) {
            return new ResourceValidator.ValidString(Arrays.asList(validStrings), true);
        }

        @Override
        public void ensureValid(String name, Object o) {
            if (o == null) {
                if (optional)
                    return;
                throw new FieldValidationException(name, null, "Value must be non-null");
            }
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new FieldValidationException(name, o, "String must be one of: " + String.join(", ", validStrings));
            }

        }

        public String toString() {
            return "[" + String.join(", ", validStrings) + "]";
        }
    }

    @Serdeable
    public static class NonEmptyString implements ResourceValidator.Validator {
        @Override
        public void ensureValid(String name, Object o) {
            if (o == null)
                throw new FieldValidationException(name, null, "Value must be non-null");
            String s = (String) o;
            if (s.isEmpty()) {
                throw new FieldValidationException(name, o, "String must be non-empty");
            }
        }

        @Override
        public String toString() {
            return "non-empty string";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            return obj instanceof NonEmptyString;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

    @Data
    @Serdeable
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
            for (ResourceValidator.Validator validator: validators) {
                validator.ensureValid(name, value);
            }
        }

        @Override
        public String toString() {
            if (validators == null) return "";
            StringBuilder desc = new StringBuilder();
            for (ResourceValidator.Validator v: validators) {
                if (!desc.isEmpty()) {
                    desc.append(',').append(' ');
                }
                desc.append(v);
            }
            return desc.toString();
        }
    }
}
