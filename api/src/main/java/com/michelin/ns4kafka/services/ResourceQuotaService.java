package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.*;

@Slf4j
@Singleton
public class ResourceQuotaService {
    /**
     * Quota response format
     */
    private static final String QUOTA_RESPONSE_FORMAT = "%s/%s";

    /**
     * No quota response format
     */
    private static final String NO_QUOTA_RESPONSE_FORMAT = "%s";

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
     * Connector service
     */
    @Inject
    KafkaConnectService kafkaConnectService;

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
     * @param resourceQuota The resource quota to create
     * @return The created resource quota
     */
    public ResourceQuota create(ResourceQuota resourceQuota) { return resourceQuotaRepository.create(resourceQuota); }

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

        Arrays.stream(values()).forEach(quotaKey -> {
            if (resourceQuota.getSpec().get(quotaKey.getKey()) != null) {
                int used = getCurrentUsedResource(namespace, quotaKey);
                int limit = Integer.parseInt(resourceQuota.getSpec().get(quotaKey.getKey()));
                if (used > limit) {
                    errors.add(String.format("Quota already exceeded for %s: %s/%s (used/limit)", quotaKey, used, limit));
                }
            }
        });

        return errors;
    }

    /**
     * For a given quota key, get the current value used by the namespace
     * @param namespace The namespace
     * @param key The quota key
     * @return The current value
     */
    public Integer getCurrentUsedResource(Namespace namespace, ResourceQuota.ResourceQuotaSpecKey key) {
        if (key.equals(COUNT_TOPICS)) {
            return topicService.findAllForNamespace(namespace).size();
        }

        if (key.equals(COUNT_PARTITIONS)) {
            return topicService.findAllForNamespace(namespace)
                    .stream()
                    .map(topic -> topic.getSpec().getPartitions())
                    .reduce(0, Integer::sum);
        }

        if (key.equals(COUNT_CONNECTORS)) {
            return kafkaConnectService.findAllForNamespace(namespace).size();
        }

        return 0;
    }

    /**
     * Validate the topic quota
     * @param namespace The namespace
     * @param topic The topic
     * @return A list of errors
     */
    public List<String> validateTopicQuota(Namespace namespace, Topic topic) {
        Optional<ResourceQuota> resourceQuotaOptional = findByNamespace(namespace.getMetadata().getName());
        if (resourceQuotaOptional.isEmpty()) {
            return List.of();
        }

        List<String> errors = new ArrayList<>();
        ResourceQuota resourceQuota = resourceQuotaOptional.get();

        if (StringUtils.isNotBlank(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()))) {
            int used = getCurrentUsedResource(namespace, COUNT_TOPICS);
            int limit = Integer.parseInt(resourceQuota.getSpec().get(COUNT_TOPICS.toString()));
            if (used + 1 > limit) {
                errors.add(String.format("Exceeding quota for %s: %s/%s (used/limit). Cannot add 1 topic.", COUNT_TOPICS, used, limit));
            }
        }

        if (StringUtils.isNotBlank(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()))) {
            int used = getCurrentUsedResource(namespace, COUNT_PARTITIONS);
            int limit = Integer.parseInt(resourceQuota.getSpec().get(COUNT_PARTITIONS.toString()));
            if (used + topic.getSpec().getPartitions() > limit) {
                errors.add(String.format("Exceeding quota for %s: %s/%s (used/limit). Cannot add %s partition(s).", COUNT_PARTITIONS, used, limit, topic.getSpec().getPartitions()));
            }
        }

        return errors;
    }

    /**
     * Validate the connector quota
     * @param namespace The namespace
     * @return A list of errors
     */
    public List<String> validateConnectorQuota(Namespace namespace) {
        Optional<ResourceQuota> resourceQuotaOptional = findByNamespace(namespace.getMetadata().getName());
        if (resourceQuotaOptional.isEmpty()) {
            return List.of();
        }

        List<String> errors = new ArrayList<>();
        ResourceQuota resourceQuota = resourceQuotaOptional.get();

        if (StringUtils.isNotBlank(resourceQuota.getSpec().get(COUNT_CONNECTORS.getKey()))) {
            int used = getCurrentUsedResource(namespace, COUNT_CONNECTORS);
            int limit = Integer.parseInt(resourceQuota.getSpec().get(COUNT_CONNECTORS.toString()));
            if (used + 1 > limit) {
                errors.add(String.format("Exceeding quota for %s: %s/%s (used/limit). Cannot add 1 connector.", COUNT_CONNECTORS, used, limit));
            }
        }

        return errors;
    }

    /**
     * Map a given optional quota to quota response format
     * @param namespace The namespace
     * @param resourceQuota The quota to map
     * @return A list of quotas as response format
     */
    public ResourceQuotaResponse toResponse(Namespace namespace, Optional<ResourceQuota> resourceQuota) {
        String countTopic = resourceQuota.isPresent() ? String.format(QUOTA_RESPONSE_FORMAT, getCurrentUsedResource(namespace, COUNT_TOPICS),
                resourceQuota.map(quota -> quota.getSpec().get(COUNT_TOPICS.toString())).orElse(UNLIMITED_QUOTA)) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, getCurrentUsedResource(namespace, COUNT_TOPICS));

        String countPartition = resourceQuota.isPresent() ? String.format(QUOTA_RESPONSE_FORMAT, getCurrentUsedResource(namespace, COUNT_PARTITIONS),
                resourceQuota.map(quota -> quota.getSpec().get(COUNT_PARTITIONS.toString())).orElse(UNLIMITED_QUOTA)) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, getCurrentUsedResource(namespace, COUNT_PARTITIONS));

        String countConnector = resourceQuota.isPresent() ? String.format(QUOTA_RESPONSE_FORMAT, getCurrentUsedResource(namespace, COUNT_CONNECTORS),
                resourceQuota.map(quota -> quota.getSpec().get(COUNT_CONNECTORS.toString())).orElse(UNLIMITED_QUOTA)) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, getCurrentUsedResource(namespace, COUNT_CONNECTORS));

        return ResourceQuotaResponse.builder()
                .metadata(resourceQuota.map(ResourceQuota::getMetadata).orElse(null))
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic(countTopic)
                        .countPartition(countPartition)
                        .countConnector(countConnector)
                        .build())
                .build();
    }
}
