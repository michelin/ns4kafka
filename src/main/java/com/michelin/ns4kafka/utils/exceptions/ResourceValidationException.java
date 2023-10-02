package com.michelin.ns4kafka.utils.exceptions;

import java.io.Serial;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Resource validation exception.
 */
@Getter
@AllArgsConstructor
public class ResourceValidationException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 32400191899153204L;

    private final List<String> validationErrors;

    private final String kind;

    private final String name;
}
