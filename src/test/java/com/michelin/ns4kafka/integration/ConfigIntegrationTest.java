package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.michelin.ns4kafka.integration.container.KafkaIntegrationTest;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;

@MicronautTest
public class ConfigIntegrationTest extends KafkaIntegrationTest {

    @Inject
    List<ManagedClusterProperties> managedClusterProperties;

    @Test
    void shouldHaveDefaultTimeouts() {
        assertNotNull(managedClusterProperties.getFirst());

        assertEquals(15000, managedClusterProperties.getFirst().getTimeout().getAcl().getCreate());
        assertEquals(15001, managedClusterProperties.getFirst().getTimeout().getAcl().getDelete());
        assertEquals(15002, managedClusterProperties.getFirst().getTimeout().getAcl().getDescribe());

        assertEquals(15003, managedClusterProperties.getFirst().getTimeout().getTopic().getAlterConfigs());
        assertEquals(15004, managedClusterProperties.getFirst().getTimeout().getTopic().getCreate());
        assertEquals(15005, managedClusterProperties.getFirst().getTimeout().getTopic().getDescribeConfigs());
        assertEquals(15006, managedClusterProperties.getFirst().getTimeout().getTopic().getDelete());
        assertEquals(15007, managedClusterProperties.getFirst().getTimeout().getTopic().getList());

        assertEquals(15008, managedClusterProperties.getFirst().getTimeout().getUser().getAlterQuotas());
        assertEquals(15009, managedClusterProperties.getFirst().getTimeout().getUser().getAlterScramCredentials());
        assertEquals(15010, managedClusterProperties.getFirst().getTimeout().getUser().getDescribeQuotas());
    }
}
