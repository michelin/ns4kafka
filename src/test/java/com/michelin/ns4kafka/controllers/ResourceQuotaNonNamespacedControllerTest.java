package com.michelin.ns4kafka.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controllers.quota.ResourceQuotaNonNamespacedController;
import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResourceQuotaNonNamespacedControllerTest {
    @InjectMocks
    ResourceQuotaNonNamespacedController resourceQuotaController;

    @Mock
    ResourceQuotaService resourceQuotaService;

    @Mock
    NamespaceService namespaceService;

    @Test
    void listAll() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuotaResponse response = ResourceQuotaResponse.builder()
            .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                .countTopic("2/5")
                .countPartition("2/10")
                .countConnector("5/5")
                .build())
            .build();

        when(namespaceService.listAll()).thenReturn(List.of(namespace));
        when(resourceQuotaService.getUsedQuotaByNamespaces(any())).thenReturn(List.of(response));

        List<ResourceQuotaResponse> actual = resourceQuotaController.listAll();
        assertEquals(1, actual.size());
        assertEquals(response, actual.get(0));
    }
}
