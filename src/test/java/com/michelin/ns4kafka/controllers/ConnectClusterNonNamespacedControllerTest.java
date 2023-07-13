package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.connect.ConnectClusterNonNamespacedController;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.services.ConnectClusterService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConnectClusterNonNamespacedControllerTest {
    @Mock
    ConnectClusterService connectClusterService;

    @InjectMocks
    ConnectClusterNonNamespacedController connectClusterNonNamespacedController;

    /**
     * Should list all Kafka Connects
     */
    @Test
    void shouldListAll() {
        ConnectCluster connectCluster = ConnectCluster.builder()
                .metadata(ObjectMeta.builder().name("connect-cluster")
                        .build())
                .build();

        when(connectClusterService.findAll(anyBoolean())).thenReturn(Flux.fromIterable(List.of(connectCluster)));

        StepVerifier.create(connectClusterNonNamespacedController.listAll(false))
            .consumeNextWith(result -> assertEquals("connect-cluster", result.getMetadata().getName()))
            .verifyComplete();
    }
}
