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
package com.michelin.ns4kafka.service.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.property.Ns4KafkaProperties.ConnectProperties;
import com.michelin.ns4kafka.property.Ns4KafkaProperties.ConnectProperties.SelfManagedProperties;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConnectorAsyncExecutorTest {
    private static final String CLUSTER_ID_TEST = "cluster_id_test";
    private static final String LOCAL_CLUSTER = "local";
    private static final String CONNECTOR_NAME = "myConnector";
    private static final String MASK = "••••••••••••";

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Mock
    ConnectProperties connectProperties;

    @Mock
    SelfManagedProperties selfManagedProperties;

    @InjectMocks
    ConnectorAsyncExecutor connectorAsyncExecutor;

    @Test
    void shouldCompareSameConnectors() {
        when(connectProperties.getSelfManaged()).thenReturn(selfManagedProperties);
        when(selfManagedProperties.getSensitiveFieldMask()).thenReturn(MASK);

        Connector expectedConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", "value1", "config2", "value2"))
                        .build())
                .build();

        Connector actualConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", "value1", "config2", "value2"))
                        .build())
                .build();

        assertTrue(connectorAsyncExecutor.connectorsAreSame(expectedConnector, actualConnector));
    }

    @Test
    void shouldCompareConnectorsWithDifferentNumberOfConfigs() {
        Connector expectedConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", "value1"))
                        .build())
                .build();

        Connector actualConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", "value1", "config2", "value2"))
                        .build())
                .build();

        assertFalse(connectorAsyncExecutor.connectorsAreSame(expectedConnector, actualConnector));
    }

    @Test
    void shouldCompareConnectorsWithDifferentConfigValues() {
        when(connectProperties.getSelfManaged()).thenReturn(selfManagedProperties);
        when(selfManagedProperties.getSensitiveFieldMask()).thenReturn(MASK);

        Connector expectedConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", "differentValue1", "config2", "value2"))
                        .build())
                .build();

        Connector actualConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", "value1", "config2", "value2"))
                        .build())
                .build();

        assertFalse(connectorAsyncExecutor.connectorsAreSame(expectedConnector, actualConnector));
    }

    @Test
    void shouldCompareConnectorsWithDifferentConfigKeys() {
        when(connectProperties.getSelfManaged()).thenReturn(selfManagedProperties);
        when(selfManagedProperties.getSensitiveFieldMask()).thenReturn(MASK);

        Connector expectedConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("differentConfig1", "value1", "config2", "value2"))
                        .build())
                .build();

        Connector actualConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", "value1", "config2", "value2"))
                        .build())
                .build();

        assertFalse(connectorAsyncExecutor.connectorsAreSame(expectedConnector, actualConnector));
    }

    @Test
    void shouldCompareConnectorsWithMaskedConfigFromConnect() {
        when(connectProperties.getSelfManaged()).thenReturn(selfManagedProperties);
        when(selfManagedProperties.getSensitiveFieldMask()).thenReturn(MASK);

        Connector expectedConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", "value1", "config2", "value2"))
                        .build())
                .build();

        Connector actualConnector = Connector.builder()
                .metadata(Metadata.builder().name(CONNECTOR_NAME).build())
                .spec(Connector.ConnectorSpec.builder()
                        .config(Map.of("config1", MASK, "config2", "value2"))
                        .build())
                .build();

        assertTrue(connectorAsyncExecutor.connectorsAreSame(expectedConnector, actualConnector));
    }
}
