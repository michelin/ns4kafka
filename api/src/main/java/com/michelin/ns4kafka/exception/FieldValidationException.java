package com.michelin.ns4kafka.exception;

public class FieldValidationException extends RuntimeException {
    private static final long serialVersionUID = 6223587833587267232L;

    public FieldValidationException(String name, Object value, String message) {
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }
}
