/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.integration.container;

import io.micronaut.core.annotation.NonNull;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Base class for Kafka Connect integration tests. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class KafkaConnectIntegrationTest extends KafkaIntegrationTest {
    private final GenericContainer<?> connect = new GenericContainer<>(
                    DockerImageName.parse("confluentinc/cp-kafka-connect:" + CONFLUENT_PLATFORM_VERSION))
            .withNetwork(NETWORK)
            .withNetworkAliases("connect")
            .withExposedPorts(8083)
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", "broker:9093")
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
            .withEnv("CONNECT_GROUP_ID", "compose-connect-group-" + UUID.randomUUID())
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "docker-connect-configs")
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "docker-connect-offsets")
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "docker-connect-status")
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/filestream-connectors")
            .withEnv("CONNECT_SASL_MECHANISM", "PLAIN")
            .withEnv("CONNECT_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
            .withEnv(
                    "CONNECT_SASL_JAAS_CONFIG",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";")
            .withEnv("CONNECT_PRODUCER_SASL_MECHANISM", "PLAIN")
            .withEnv("CONNECT_PRODUCER_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
            .withEnv(
                    "CONNECT_PRODUCER_SASL_JAAS_CONFIG",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";")
            .withEnv("CONNECT_CONSUMER_SASL_MECHANISM", "PLAIN")
            .withEnv("CONNECT_CONSUMER_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
            .withEnv(
                    "CONNECT_CONSUMER_SASL_JAAS_CONFIG",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";")
            .waitingFor(Wait.forHttp("/").forStatusCode(200));

    @NonNull @Override
    public Map<String, String> getProperties() {
        Map<String, String> brokerProperties = super.getProperties();

        if (!connect.isRunning()) {
            connect.start();
        }

        Map<String, String> properties = new HashMap<>(brokerProperties);
        properties.put("ns4kafka.managed-clusters.test-cluster.connects.test-connect.url", getConnectUrl());
        return properties;
    }

    protected String getConnectUrl() {
        return "http://" + connect.getHost() + ":" + connect.getFirstMappedPort();
    }
}
