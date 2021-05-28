package com.michelin.ns4kafka.controllers;

public class KafkaConsumerException extends RuntimeException {

   protected KafkaConsumerException() {
   }

   protected KafkaConsumerException(String message) {
      super(message);
   }

}
