package com.michelin.ns4kafka.validation;

import lombok.Getter;

import java.util.List;

public class ResourceValidationException extends RuntimeException {
    @Getter
    List<String> validationErrors;
    public ResourceValidationException(List<String> validationsErrors){
        this.validationErrors=validationsErrors;
    }
}
