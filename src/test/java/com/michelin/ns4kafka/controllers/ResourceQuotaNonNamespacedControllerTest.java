package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.quota.ResourceQuotaNonNamespacedController;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResourceQuotaNonNamespacedControllerTest {
    @InjectMocks
    ResourceQuotaNonNamespacedController resourceQuotaController;

    @Mock
    ResourceQuotaService resourceQuotaService;

    /**
     * Validate quota listing
     */
    @Test
    void listAll() {
        ResourceQuotaResponse response = ResourceQuotaResponse.builder()
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic("2/5")
                        .countPartition("2/10")
                        .countConnector("5/5")
                        .build())
                .build();

        when(resourceQuotaService.getUsedResourcesByQuotaForAllNamespaces()).thenReturn(List.of(response));

        List<ResourceQuotaResponse> actual = resourceQuotaController.listAll();
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(response, actual.get(0));
    }
}
