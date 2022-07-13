package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.*;

@Slf4j
@Singleton
public class ResourceQuotaService {
    /**
     * Format of the quota response
     */
    private static final String QUOTA_RESPONSE_FORMAT = "%s / %s";

    /**
     * Limit to display when there is no quota
     */
    private static final String UNLIMITED_QUOTA = "INF";

    /**
     * Role binding repository
     */
    @Inject
    ResourceQuotaRepository resourceQuotaRepository;

    /**
     * Topic service
     */
    @Inject
    TopicService topicService;

    /**
     * Find a resource quota by namespace
     * @param namespace The namespace used to research
     * @return The researched resource quota
     */
    public Optional<ResourceQuota> findByNamespace(String namespace) {
        return resourceQuotaRepository.findForNamespace(namespace);
    }

    /**
     * Find a resource quota by namespace and name
     * @param namespace The namespace
     * @param quota The quota name
     * @return The researched resource quota
     */
    public Optional<ResourceQuota> findByName(String namespace, String quota) {
        return findByNamespace(namespace)
                .stream()
                .filter(resourceQuota -> resourceQuota.getMetadata().getName().equals(quota))
                .findFirst();
    }

    /**
     * Create a resource quota
     * @param resourceQuota The resource to create
     */
    public void create(ResourceQuota resourceQuota) { resourceQuotaRepository.create(resourceQuota); }

    /**
     * Delete a resource quota
     * @param resourceQuota The resource quota to delete
     */
    public void delete(ResourceQuota resourceQuota) {
        resourceQuotaRepository.delete(resourceQuota);
    }

    /**
     * Validate a given new resource quota against the current resource used by the namespace
     * @param namespace The namespace
     * @param resourceQuota The new resource quota
     * @return A list of validation errors
     */
    public List<String> validateNewQuotaAgainstCurrentResource(Namespace namespace, ResourceQuota resourceQuota) {
        List<String> errors = new ArrayList<>();

        if (resourceQuota.getSpec().get(COUNT_TOPICS.toString()) != null) {
            int countTopics = getCurrentUsedResource(namespace, COUNT_TOPICS);
            int quotaCountTopics = Integer.parseInt(resourceQuota.getSpec().get(COUNT_TOPICS.toString()));
            if (countTopics > quotaCountTopics) {
                errors.add(String.format("The quota %s for %s already exceeds the number of topics %s.", quotaCountTopics, COUNT_TOPICS,
                        countTopics));
            }
        }

        return errors;
    }

    /**
     * For a given quota key, get the current value used by the namespace
     * @param namespace The namespace
     * @param key The quota key
     * @return The current value
     */
    private Integer getCurrentUsedResource(Namespace namespace, ResourceQuota.ResourceQuotaSpecKey key) {
        if (key.equals(COUNT_TOPICS)) {
            return topicService.findAllForNamespace(namespace).size();
        }

        return 0;
    }

    /**
     * Map a given optional quota to quota response format
     * @param namespace The namespace
     * @param resourceQuota The quota to map
     * @return A list of quotas as response format
     */
    public ResourceQuotaResponse toResponse(Namespace namespace, Optional<ResourceQuota> resourceQuota) {
        return ResourceQuotaResponse.builder()
                .metadata(resourceQuota.map(ResourceQuota::getMetadata).orElse(null))
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic(String.format(QUOTA_RESPONSE_FORMAT,
                                getCurrentUsedResource(namespace, COUNT_TOPICS),
                                resourceQuota.map(quota -> quota.getSpec().get(COUNT_TOPICS.toString())).orElse(UNLIMITED_QUOTA)))
                        .countPartition(String.format(QUOTA_RESPONSE_FORMAT,
                                getCurrentUsedResource(namespace, COUNT_PARTITIONS),
                                resourceQuota.map(quota -> quota.getSpec().get(COUNT_PARTITIONS.toString())).orElse(UNLIMITED_QUOTA)))
                        .countConnector(String.format(QUOTA_RESPONSE_FORMAT,
                                getCurrentUsedResource(namespace, COUNT_CONNECTORS),
                                resourceQuota.map(quota -> quota.getSpec().get(COUNT_CONNECTORS.toString())).orElse(UNLIMITED_QUOTA)))
                        .build())
                .build();
    }
}
