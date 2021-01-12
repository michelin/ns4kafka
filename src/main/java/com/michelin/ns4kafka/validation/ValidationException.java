package com.michelin.ns4kafka.validation;

import lombok.Getter;

import java.util.List;

public class ValidationException extends RuntimeException {
    @Getter
    List<String> validationErrors;
    public ValidationException(String name, Object value, String message){
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }
    public ValidationException(String message){
        super(message);
    }
    public ValidationException(List<String> validationsErrors){
        this.validationErrors=validationsErrors;
    }
}
