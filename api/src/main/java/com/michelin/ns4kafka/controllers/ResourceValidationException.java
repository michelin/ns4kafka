package com.michelin.ns4kafka.controllers;

import lombok.Getter;

import java.util.List;

import com.michelin.ns4kafka.models.Status.StatusCauses;

public class ResourceValidationException extends RuntimeException {
    private static final long serialVersionUID = 32400191899153204L;
    @Getter
    private final List<StatusCauses> validationErrors;
    @Getter
    private final String kind;
    @Getter
    private final String name;

    public ResourceValidationException(List<StatusCauses> validationErrors, String kind, String name) {
        this.validationErrors = validationErrors;
        this.kind = kind;
        this.name = name;
    }

}
