package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.COUNT_TOPICS;

@Slf4j
@Singleton
public class ResourceQuotaService {
    /**
     * The role binding repository
     */
    @Inject
    ResourceQuotaRepository resourceQuotaRepository;

    /**
     * The topic service
     */
    @Inject
    TopicService topicService;

    /**
     * Find a resource quota by name
     * @param namespace The namespace used to research
     * @return The researched role binding
     */
    public Optional<ResourceQuota> findByNamespace(String namespace) {
        return resourceQuotaRepository.findForNamespace(namespace);
    }

    /**
     * Create a resource quota
     * @param resourceQuota The resource to create
     */
    public void create(ResourceQuota resourceQuota) { resourceQuotaRepository.create(resourceQuota); }

    /**
     * Validate a given new resource quota against the current resource used by the namespace
     * @param namespace The namespace
     * @param newResourceQuota The new resource quota
     * @return A list of validation errors
     */
    public List<String> validateNewQuotaAgainstCurrentResource(Namespace namespace, ResourceQuota newResourceQuota) {
        List<String> errors = new ArrayList<>();

        int countTopics = getCurrentValueForQuotaKey(namespace, COUNT_TOPICS);
        int quotaCountTopics = Integer.parseInt(newResourceQuota.getSpec().get(COUNT_TOPICS));
        if (countTopics > quotaCountTopics) {
            errors.add(String.format("The quota %s for %s already exceeds the number of topics %s.", quotaCountTopics, COUNT_TOPICS,
                    countTopics));
        }

        return errors;
    }

    /**
     * For a given quota key, get the current value used by the namespace
     * @param namespace The namespace
     * @param key The quota key
     * @return The current value
     */
    private int getCurrentValueForQuotaKey(Namespace namespace, ResourceQuota.ResourceQuotaSpecKey key) {
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
    public List<ResourceQuotaResponse> mapQuotaToQuotaResponse(Namespace namespace, Optional<ResourceQuota> resourceQuota) {
        List<ResourceQuotaResponse> quotaResponse = new ArrayList<>();

        String countTopics = String.valueOf(getCurrentValueForQuotaKey(namespace, COUNT_TOPICS));
        quotaResponse.add(ResourceQuotaResponse.builder()
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .resourceName(COUNT_TOPICS)
                        .used(countTopics)
                        .limit(resourceQuota.isPresent() ? resourceQuota.get().getSpec().get(COUNT_TOPICS) : StringUtils.EMPTY)
                        .build())
                .build());

        return quotaResponse;
    }

    /**
     * Map the current resource quota of the given namespace to a resource quota response format
     * @param namespace The namespace
     * @return A list of quotas as response format
     */
    public List<ResourceQuotaResponse> mapCurrentQuotaToQuotaResponse(Namespace namespace) {
        return mapQuotaToQuotaResponse(namespace, findByNamespace(namespace.getMetadata().getName()));
    }
}
