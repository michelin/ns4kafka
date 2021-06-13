package com.michelin.ns4kafka.integration;

import org.apache.kafka.clients.admin.Admin;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class KafkaCluster {
    static org.testcontainers.containers.KafkaContainer kafka;
    static Admin adminClient;

    static void init() {
        if (kafka == null) {
            kafka = new org.testcontainers.containers.KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"));
            kafka.start();
        }
    }

    static Admin getAdminClient() {
        if (adminClient == null)
            adminClient = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()));
        return adminClient;
    }
}
