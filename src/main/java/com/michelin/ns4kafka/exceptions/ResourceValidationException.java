package com.michelin.ns4kafka.exceptions;

import lombok.Getter;

import java.util.List;

public class ResourceValidationException extends RuntimeException {
    @Getter
    List<String> validationErrors;
    public ResourceValidationException(String name, Object value, String message){
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }
    public ResourceValidationException(String message){
        super(message);
    }
    public ResourceValidationException(List<String> validationsErrors){
        this.validationErrors=validationsErrors;
    }
}
