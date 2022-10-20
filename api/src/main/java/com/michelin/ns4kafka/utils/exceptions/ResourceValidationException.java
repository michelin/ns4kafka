package com.michelin.ns4kafka.utils.exceptions;

import lombok.Getter;

import java.util.List;


public class ResourceValidationException extends RuntimeException {
    private static final long serialVersionUID = 32400191899153204L;
    @Getter
    private final List<String> validationErrors;

    @Getter
    private final String kind;
    @Getter
    private final String name;

    public ResourceValidationException(List<String> validationErrors, String kind, String name) {
        this.validationErrors = validationErrors;
        this.kind = kind;
        this.name = name;
    }

}
