package com.michelin.ns4kafka.repositories.kafka;

public class StoreInitializationException extends Exception {

    public StoreInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public StoreInitializationException(String message) {
        super(message);
    }

    public StoreInitializationException(Throwable cause) {
        super(cause);
    }

    public StoreInitializationException() {
        super();
    }
}
