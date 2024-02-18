package com.michelin.ns4kafka.utils.exceptions;

import java.io.Serial;
import java.util.List;
import lombok.Getter;

/**
 * Resource validation exception.
 */
@Getter
public class ResourceValidationException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 32400191899153204L;

    private final List<String> validationErrors;

    private final String kind;

    private final String name;

    /**
     * Constructor.
     *
     * @param kind            The kind of the resource
     * @param name            The name of the resource
     * @param validationError The validation error
     */
    public ResourceValidationException(String kind, String name, String validationError) {
        this.kind = kind;
        this.name = name;
        this.validationErrors = List.of(validationError);
    }

    /**
     * Constructor.
     *
     * @param kind             The kind of the resource
     * @param name             The name of the resource
     * @param validationErrors The validation errors
     */
    public ResourceValidationException(String kind, String name, List<String> validationErrors) {
        this.kind = kind;
        this.name = name;
        this.validationErrors = validationErrors;
    }
}
