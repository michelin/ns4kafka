package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.quota.ResourceQuota;

import java.util.List;
import java.util.Optional;

public interface ResourceQuotaRepository {
    /**
     * Find all quotas of all namespaces
     * @return The resource quotas
     */
    List<ResourceQuota> findAll();

    /**
     * Get resource quota by namespace
     * @param namespace The namespace used to research
     * @return The resource quotas associated to the namespace
     */
    Optional<ResourceQuota> findForNamespace(String namespace);

    /**
     * Create a resource quota
     * @param resourceQuota The resource quota to create
     */
    ResourceQuota create(ResourceQuota resourceQuota);

    /**
     * Delete a resource quota
     * @param resourceQuota The resource quota to delete
     */
    void delete(ResourceQuota resourceQuota);
}
