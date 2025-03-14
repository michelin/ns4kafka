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
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Base class for Schema Registry integration tests. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SchemaRegistryIntegrationTest extends KafkaIntegrationTest {
    private final GenericContainer<?> schemaRegistry = new GenericContainer<>(
                    DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION))
            .withNetwork(NETWORK)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:9093")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM", "PLAIN")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
            .withEnv(
                    "SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @NonNull @Override
    public Map<String, String> getProperties() {
        Map<String, String> brokerProperties = super.getProperties();

        if (!schemaRegistry.isRunning()) {
            schemaRegistry.start();
        }

        Map<String, String> properties = new HashMap<>(brokerProperties);
        properties.put("ns4kafka.managed-clusters.test-cluster.schemaRegistry.url", getSchemaRegistryUrl());
        return properties;
    }

    protected String getSchemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort();
    }
}
