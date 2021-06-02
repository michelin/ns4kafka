package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ConnectorTest {
    @Test
    void testEquals() {
        Connector.ConnectorStatus originalStatus = Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.RUNNING)
                .build();
        Connector.ConnectorStatus sameStatus = Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.FAILED)
                .build();

        Connector original = Connector.builder()
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("cluster1")
                        .config(Map.of("k1", "v1",
                                "k2", "v2"))
                        .build())
                .status(originalStatus)
                .build();

        Connector same = Connector.builder()
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("cluster1")
                        // inverted map
                        .config(Map.of("k2", "v2",
                                "k1", "v1"))
                        .build())
                // different status
                .status(sameStatus)
                .build();

        Connector different = Connector.builder()
                .spec(Connector.ConnectorSpec.builder()
                        .connectCluster("cluster2")
                        // inverted map
                        .config(Map.of("k2", "v2",
                                "k1", "v1"))
                        .build())
                // different status
                .status(sameStatus)
                .build();

        // objects are same, even if status differs
        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(originalStatus, sameStatus);

        Assertions.assertNotEquals(original, different);

    }
}
