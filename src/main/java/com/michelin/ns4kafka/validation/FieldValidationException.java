package com.michelin.ns4kafka.validation;

import java.io.Serial;

/**
 * Field validation exception.
 */
public class FieldValidationException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 6223587833587267232L;

    public FieldValidationException(String name, Object value, String message) {
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }
}
