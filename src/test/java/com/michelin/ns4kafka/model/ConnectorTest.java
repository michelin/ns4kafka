package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.michelin.ns4kafka.model.connector.Connector;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConnectorTest {
    @Test
    void shouldBeEqual() {
        Connector original = Connector.builder()
            .metadata(Metadata.builder()
                .name("connect1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster1")
                .config(Map.of(
                    "k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.RUNNING)
                .build())
            .build();

        Connector same = Connector.builder()
            .metadata(Metadata.builder()
                .name("connect1")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster1")
                // inverted map
                .config(Map.of(
                    "k2", "v2",
                    "k1", "v1"))
                .build())
            // different status
            .status(Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.FAILED)
                .build())
            .build();

        Connector differentByConnectCluster = Connector.builder()
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster2")
                .config(Map.of(
                    "k1", "v1",
                    "k2", "v2"))
                .build())
            .build();

        Connector differentByConfig = Connector.builder()
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster2")
                .config(Map.of(
                    "k1", "v1",
                    "k2", "v2",
                    "k3", "v3"))
                .build())
            .build();

        // objects are same, even if status differs
        assertEquals(original, same);

        assertNotEquals(original, differentByConnectCluster);
        assertNotEquals(original, differentByConfig);

        Connector differentByMetadata = Connector.builder()
            .metadata(Metadata.builder()
                .name("connect2")
                .build())
            .spec(Connector.ConnectorSpec.builder()
                .connectCluster("cluster1")
                .config(Map.of("k1", "v1",
                    "k2", "v2"))
                .build())
            .status(Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.RUNNING)
                .build())
            .build();

        assertNotEquals(original, differentByMetadata);
    }
}
