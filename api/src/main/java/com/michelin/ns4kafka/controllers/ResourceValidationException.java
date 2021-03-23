package com.michelin.ns4kafka.controllers;

import lombok.Getter;

import java.util.List;

public class ResourceValidationException extends RuntimeException {
    private static final long serialVersionUID = 32400191899153204L;
    @Getter
    private final List<String> validationErrors;

    public ResourceValidationException(List<String> validationsErrors) {
        this.validationErrors = validationsErrors;
    }
}
