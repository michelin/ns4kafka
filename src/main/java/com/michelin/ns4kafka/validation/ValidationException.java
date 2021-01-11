package com.michelin.ns4kafka.validation;

public class ValidationException extends RuntimeException {
    public ValidationException(String name, Object value, String message){
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }
    public ValidationException(String message){
        super(message);
    }
}
