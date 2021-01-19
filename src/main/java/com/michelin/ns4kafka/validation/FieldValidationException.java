package com.michelin.ns4kafka.validation;

import lombok.Getter;

import java.util.List;

public class FieldValidationException extends RuntimeException {
    public FieldValidationException(String name, Object value, String message){
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }
}
