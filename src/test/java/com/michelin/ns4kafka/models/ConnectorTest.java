package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.models.connector.Connector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class ConnectorTest {
    @Test
    void testEquals() {
        Connector original = Connector.builder()
                .metadata(ObjectMeta.builder()
                        .name("connect1")
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

        Connector same = Connector.builder()
                .metadata(ObjectMeta.builder()
                        .name("connect1")
                        .build())
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("cluster1")
                        // inverted map
                        .config(Map.of("k2", "v2",
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
                        .config(Map.of("k1", "v1",
                                "k2", "v2"))
                        .build())
                .build();
        Connector differentByConfig = Connector.builder()
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("cluster2")
                        .config(Map.of("k1", "v1",
                                "k2", "v2",
                                "k3", "v3"))
                        .build())
                .build();
        Connector differentByMetadata = Connector.builder()
                .metadata(ObjectMeta.builder()
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

        // objects are same, even if status differs
        Assertions.assertEquals(original, same);

        Assertions.assertNotEquals(original, differentByConnectCluster);
        Assertions.assertNotEquals(original, differentByConfig);
        Assertions.assertNotEquals(original, differentByMetadata);

    }
}
