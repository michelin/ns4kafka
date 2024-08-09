package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controller.connect.ConnectClusterNonNamespacedController;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.service.ConnectClusterService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ConnectClusterNonNamespacedControllerTest {
    @Mock
    ConnectClusterService connectClusterService;

    @InjectMocks
    ConnectClusterNonNamespacedController connectClusterNonNamespacedController;

    @Test
    void shouldListAll() {
        ConnectCluster connectCluster = ConnectCluster.builder()
            .metadata(Metadata.builder().name("connect-cluster")
                .build())
            .build();

        when(connectClusterService.findAll(anyBoolean())).thenReturn(Flux.fromIterable(List.of(connectCluster)));

        StepVerifier.create(connectClusterNonNamespacedController.listAll(false))
            .consumeNextWith(result -> assertEquals("connect-cluster", result.getMetadata().getName()))
            .verifyComplete();
    }
}
