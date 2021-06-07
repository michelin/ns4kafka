package com.michelin.ns4kafka.validation;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.*;

import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
@NoArgsConstructor
@Data
public abstract class ResourceValidator {

    protected Map<String,Validator> validationConstraints;

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
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
    @NoArgsConstructor
    public static class Range implements ResourceValidator.Validator {
        private Number min;
        private Number max;

        /**
         *  A numeric range with inclusive upper bound and inclusive lower bound
         * @param min  the lower bound
         * @param max  the upper bound
         */
        public Range(Number min, Number max) {
            this.min = min;
            this.max = max;
        }

        /**
         * A numeric range that checks only the lower bound
         *
         * @param min The minimum acceptable value
         */
        public static ResourceValidator.Range atLeast(Number min) {
            return new ResourceValidator.Range(min, null);
        }

        /**
         * A numeric range that checks both the upper (inclusive) and lower bound
         */
        public static ResourceValidator.Range between(Number min, Number max) {
            return new ResourceValidator.Range(min, max);
        }

        public void ensureValid(String name, Object o) {
            Number n = null;
            if (o == null)
                throw new FieldValidationException(name, null, "Value must be non-null");
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
    @NoArgsConstructor
    public static class ValidList implements ResourceValidator.Validator {

        List<String> validStrings;

        public ValidList(List<String> validStrings) {
            this.validStrings = validStrings;
        }

        public static ResourceValidator.ValidList in(String... validStrings) {
            return new ResourceValidator.ValidList(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(final String name, final Object o) {
            if (o == null)
                throw new FieldValidationException(name, null, "Value must be non-null");
            String s = (String)o;

            List<String> values = List.of(s); //default if no "," (most of the time)
            if(s.contains(",")){
                //split and strip
                values = Arrays.stream(s.split(",")).map(item -> item.strip()).collect(Collectors.toList());
            }

            for (String string : values) {
                ValidString validString = new ValidString(validStrings);
                validString.ensureValid(name, string);
            }
        }

        public String toString() {
            return validStrings.toString();
        }
    }

    @Data
    @NoArgsConstructor
    public static class ValidString implements ResourceValidator.Validator {
        List<String> validStrings;


        public ValidString(List<String> validStrings) {
            this.validStrings = validStrings;
        }

        public static ResourceValidator.ValidString in(String... validStrings) {
            return new ResourceValidator.ValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            if (o == null)
                throw new FieldValidationException(name, null, "Value must be non-null");
            String s = (String) o;
            if (!validStrings.contains(s)) {
                throw new FieldValidationException(name, o, "String must be one of: " + String.join(", ", validStrings));
            }

        }

        public String toString() {
            return "[" + String.join(", ", validStrings) + "]";
        }
    }

    public static class NonEmptyString implements ResourceValidator.Validator {

        @Override
        public void ensureValid(String name, Object o) {
            if (o == null)
                throw new FieldValidationException(name, null, "Value must be non-null");
            String s = (String) o;
            if (s != null && s.isEmpty()) {
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
            if (!(obj instanceof NonEmptyString)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }

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
            for (ResourceValidator.Validator validator: validators) {
                validator.ensureValid(name, value);
            }
        }

        @Override
        public String toString() {
            if (validators == null) return "";
            StringBuilder desc = new StringBuilder();
            for (ResourceValidator.Validator v: validators) {
                if (desc.length() > 0) {
                    desc.append(',').append(' ');
                }
                desc.append(String.valueOf(v));
            }
            return desc.toString();
        }
    }
    /*
    public static class CaseInsensitiveValidString implements ResourceValidator.Validator {

        Set<String> validStrings;

        private CaseInsensitiveValidString(List<String> validStrings) {
            this.validStrings = validStrings.stream()
                    .map(s -> s.toUpperCase(Locale.ROOT))
                    .collect(Collectors.toSet());
        }

        public static ResourceValidator.CaseInsensitiveValidString in(String... validStrings) {
            return new ResourceValidator.CaseInsensitiveValidString(Arrays.asList(validStrings));
        }

        @Override
        public void ensureValid(String name, Object o) {
            String s = (String) o;
            if (s == null || !validStrings.contains(s.toUpperCase(Locale.ROOT))) {
                throw new ValidationException(name, o, "String must be one of (case insensitive): " + String.join(", ", validStrings));
            }
        }

        public String toString() {
            return "(case insensitive) [" + String.join(", ", validStrings) + "]";
        }
    }

    public static class NonNullValidator implements ResourceValidator.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                // Pass in the string null to avoid the spotbugs warning
                throw new ValidationException(name, "null", "entry must be non null");
            }
        }

        public String toString() {
            return "non-null string";
        }
    }

    public static class LambdaValidator implements ResourceValidator.Validator {
        BiConsumer<String, Object> ensureValid;
        Supplier<String> toStringFunction;

        private LambdaValidator(BiConsumer<String, Object> ensureValid,
                                Supplier<String> toStringFunction) {
            this.ensureValid = ensureValid;
            this.toStringFunction = toStringFunction;
        }

        public static ResourceValidator.LambdaValidator with(BiConsumer<String, Object> ensureValid,
                                                             Supplier<String> toStringFunction) {
            return new ResourceValidator.LambdaValidator(ensureValid, toStringFunction);
        }

        @Override
        public void ensureValid(String name, Object value) {
            ensureValid.accept(name, value);
        }

        @Override
        public String toString() {
            return toStringFunction.get();
        }
    }

    public static class CompositeValidator implements ResourceValidator.Validator {
        private final List<ResourceValidator.Validator> validators;

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
                if (desc.length() > 0) {
                    desc.append(',').append(' ');
                }
                desc.append(String.valueOf(v));
            }
            return desc.toString();
        }
    }

    public static class NonEmptyStringWithoutControlChars implements ResourceValidator.Validator {

        public static ResourceValidator.NonEmptyStringWithoutControlChars nonEmptyStringWithoutControlChars() {
            return new ResourceValidator.NonEmptyStringWithoutControlChars();
        }

        @Override
        public void ensureValid(String name, Object value) {
            String s = (String) value;

            if (s == null) {
                // This can happen during creation of the config object due to no default value being defined for the
                // name configuration - a missing name parameter is caught when checking for mandatory parameters,
                // thus we can ok a null value here
                return;
            } else if (s.isEmpty()) {
                throw new ValidationException(name, value, "String may not be empty");
            }

            // Check name string for illegal characters
            ArrayList<String> foundIllegalCharacters = new ArrayList<>();

            for (int i = 0; i < s.length(); i++) {
                if (Character.isISOControl(s.codePointAt(i))) {
                    foundIllegalCharacters.add(String.valueOf(s.codePointAt(i)));
                }
            }

            if (!foundIllegalCharacters.isEmpty()) {
                throw new ValidationException(name, value, "String may not contain control sequences but had the following ASCII chars: " + String.join(", ", foundIllegalCharacters));
            }
        }

        public String toString() {
            return "non-empty string without ISO control characters";
        }
    }

     */
}
