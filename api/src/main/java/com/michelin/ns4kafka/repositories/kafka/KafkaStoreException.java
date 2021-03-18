package com.michelin.ns4kafka.repositories.kafka;

public class KafkaStoreException extends RuntimeException {

    public KafkaStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaStoreException(String message) {
        super(message);
    }

    public KafkaStoreException(Throwable cause) {
        super(cause);
    }

    public KafkaStoreException() {
        super();
    }
}
