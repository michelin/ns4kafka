package com.michelin.ns4kafka.validation;

import java.io.Serial;

/**
 * Field validation exception.
 */
public class FieldValidationException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 6223587833587267232L;

    /**
     * Constructor.
     *
     * @param error The error message
     */
    public FieldValidationException(String error) {
        super(error);
    }
}
