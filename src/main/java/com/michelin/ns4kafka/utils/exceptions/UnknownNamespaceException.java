package com.michelin.ns4kafka.utils.exceptions;

/**
 * Exception thrown when a namespace is unknown.
 */
public class UnknownNamespaceException extends RuntimeException {
    private static final String MESSAGE = "Accessing unknown namespace \"%s\"";

    public UnknownNamespaceException(String namespace) {
        super(String.format(MESSAGE, namespace));
    }
}
