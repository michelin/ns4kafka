package com.michelin.ns4kafka.integration;

import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class ConnectCluster {
    static GenericContainer<?> connect;
    static void init(Map<String,String> env) {
        if (connect == null) {
            connect = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-kafka-connect:6.2.0")).withEnv(env);
            connect.start();

        }
    }

    static void close() {
        if (connect != null) {
            connect.close();
            connect = null;
        }
    }
}
