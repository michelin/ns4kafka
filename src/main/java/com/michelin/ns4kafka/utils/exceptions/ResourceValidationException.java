package com.michelin.ns4kafka.utils.exceptions;

import com.michelin.ns4kafka.models.MetadataResource;
import com.michelin.ns4kafka.utils.enums.Kind;
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

    private final Kind kind;

    private final String name;

    private final List<String> validationErrors;

    /**
     * Constructor.
     *
     * @param kind            The kind of the resource
     * @param name            The name of the resource
     * @param validationError The validation error
     */
    public ResourceValidationException(Kind kind, String name, String validationError) {
        this(kind, name, List.of(validationError));
    }

    /**
     * Constructor.
     *
     * @param resource        The resource
     * @param validationError The validation error
     */
    public ResourceValidationException(MetadataResource resource, String validationError) {
        this(resource.getKind(), resource.getMetadata().getName(), List.of(validationError));
    }

    /**
     * Constructor.
     *
     * @param resource         The resource
     * @param validationErrors The validation errors
     */
    public ResourceValidationException(MetadataResource resource, List<String> validationErrors) {
        this(resource.getKind(), resource.getMetadata().getName(), validationErrors);
    }
}
