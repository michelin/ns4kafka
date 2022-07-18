package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResourceQuotaServiceTest {
    /**
     * The resource quota service
     */
    @InjectMocks
    ResourceQuotaService resourceQuotaService;

    /**
     * The resource quota repository
     */
    @Mock
    ResourceQuotaRepository resourceQuotaRepository;

    /**
     * Test get quota by namespace when it is defined
     */
    @Test
    void findByNamespace() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.of(resourceQuota));

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByNamespace(ns.getMetadata().getName());
        Assertions.assertTrue(resourceQuotaOptional.isPresent());
        Assertions.assertEquals("test", resourceQuotaOptional.get().getMetadata().getName());
    }

    /**
     * Test get quota by namespace when it is empty
     */
    @Test
    void findByNamespaceEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of("count/topics", "1"))
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.empty());

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByNamespace(ns.getMetadata().getName());
        Assertions.assertTrue(resourceQuotaOptional.isEmpty());
    }
}
