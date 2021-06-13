package com.michelin.ns4kafka.integration;

import org.testcontainers.utility.DockerImageName;

public class KafkaContainer {
    static org.testcontainers.containers.KafkaContainer kafka;
    static void init(){
        if(kafka == null){
            kafka = new org.testcontainers.containers.KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"));
            kafka.start();
        }
    }
}
