package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import com.michelin.ns4kafka.utils.BytesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.stream.Collectors;

import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.*;
import static com.michelin.ns4kafka.utils.BytesUtils.*;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;

@Slf4j
@Singleton
public class ResourceQuotaService {
    private static final String QUOTA_ALREADY_EXCEEDED_ERROR = "Quota already exceeded for %s: %s/%s (used/limit)";
    private static final String QUOTA_RESPONSE_FORMAT = "%s/%s";
    private static final String NO_QUOTA_RESPONSE_FORMAT = "%s";

    @Inject
    NamespaceService namespaceService;

    @Inject
    ResourceQuotaRepository resourceQuotaRepository;

    @Inject
    TopicService topicService;

    @Inject
    ConnectorService connectorService;

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
    public List<String> validateNewResourceQuota(Namespace namespace, ResourceQuota resourceQuota) {
        List<String> errors = new ArrayList<>();

        if (StringUtils.isNotBlank(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()))) {
            long used = getCurrentCountTopicsByNamespace(namespace);
            long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()));
            if (used > limit) {
                errors.add(String.format(QUOTA_ALREADY_EXCEEDED_ERROR, COUNT_TOPICS, used, limit));
            }
        }

        if (StringUtils.isNotBlank(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()))) {
            long used = getCurrentCountPartitionsByNamespace(namespace);
            long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()));
            if (used > limit) {
                errors.add(String.format(QUOTA_ALREADY_EXCEEDED_ERROR, COUNT_PARTITIONS, used, limit));
            }
        }

        if (StringUtils.isNotBlank(resourceQuota.getSpec().get(DISK_TOPICS.getKey()))) {
            String limitAsString = resourceQuota.getSpec().get(DISK_TOPICS.getKey());
            if (!limitAsString.endsWith(BYTE) && !limitAsString.endsWith(KIBIBYTE) && !limitAsString.endsWith(MEBIBYTE) && !limitAsString.endsWith(GIBIBYTE)) {
                errors.add(String.format("Invalid value for %s: value must end with either %s, %s, %s or %s",
                        DISK_TOPICS, BYTE, KIBIBYTE, MEBIBYTE, GIBIBYTE));
            } else {
                long used = getCurrentDiskTopicsByNamespace(namespace);
                long limit = BytesUtils.humanReadableToBytes(limitAsString);
                if (used > limit) {
                    errors.add(String.format(QUOTA_ALREADY_EXCEEDED_ERROR, DISK_TOPICS,
                            BytesUtils.bytesToHumanReadable(used), limitAsString));
                }
            }
        }

        if (StringUtils.isNotBlank(resourceQuota.getSpec().get(COUNT_CONNECTORS.getKey()))) {
            long used = getCurrentCountConnectorsByNamespace(namespace);
            long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_CONNECTORS.getKey()));
            if (used > limit) {
                errors.add(String.format(QUOTA_ALREADY_EXCEEDED_ERROR, COUNT_CONNECTORS, used, limit));
            }
        }

        return errors;
    }

    /**
     * Get currently used number of topics by namespace
     * @param namespace The namespace
     * @return The number of topics
     */
    public long getCurrentCountTopicsByNamespace(Namespace namespace) {
        return topicService.findAllForNamespace(namespace).size();
    }

    /**
     * Get currently used number of partitions by namespace
     * @param namespace The namespace
     * @return The number of partitions
     */
    public long getCurrentCountPartitionsByNamespace(Namespace namespace) {
        return topicService.findAllForNamespace(namespace)
                .stream()
                .map(topic -> topic.getSpec().getPartitions())
                .reduce(0, Integer::sum)
                .longValue();
    }

    /**
     * Get currently used topic disk in bytes by namespace
     * @param namespace The namespace
     * @return The number of topic disk
     */
    public long getCurrentDiskTopicsByNamespace(Namespace namespace) {
        return topicService.findAllForNamespace(namespace)
                .stream()
                .map(topic -> Long.parseLong(topic.getSpec().getConfigs().getOrDefault("retention.bytes", "0")) *
                        topic.getSpec().getPartitions())
                .reduce(0L, Long::sum);
    }

    /**
     * Get currently used number of connectors by namespace
     * @param namespace The namespace
     * @return The number of connectors
     */
    public long getCurrentCountConnectorsByNamespace(Namespace namespace) {
        return connectorService.findAllForNamespace(namespace).size();
    }

    /**
     * Validate the topic quota
     * @param namespace The namespace
     * @param existingTopic The existing topic
     * @param newTopic The new topic
     * @return A list of errors
     */
    public List<String> validateTopicQuota(Namespace namespace, Optional<Topic> existingTopic, Topic newTopic) {
        Optional<ResourceQuota> resourceQuotaOptional = findByNamespace(namespace.getMetadata().getName());
        if (resourceQuotaOptional.isEmpty()) {
            return List.of();
        }

        List<String> errors = new ArrayList<>();
        ResourceQuota resourceQuota = resourceQuotaOptional.get();

        // Check count topics and count partitions only at creation
        if (existingTopic.isEmpty()) {
            if (StringUtils.isNotBlank(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()))) {
                long used = getCurrentCountTopicsByNamespace(namespace);
                long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()));
                if (used + 1 > limit) {
                    errors.add(String.format("Exceeding quota for %s: %s/%s (used/limit). Cannot add 1 topic.", COUNT_TOPICS, used, limit));
                }
            }

            if (StringUtils.isNotBlank(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()))) {
                long used = getCurrentCountPartitionsByNamespace(namespace);
                long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()));
                if (used + newTopic.getSpec().getPartitions() > limit) {
                    errors.add(String.format("Exceeding quota for %s: %s/%s (used/limit). Cannot add %s partition(s).", COUNT_PARTITIONS, used, limit, newTopic.getSpec().getPartitions()));
                }
            }
        }

        if (StringUtils.isNotBlank(resourceQuota.getSpec().get(DISK_TOPICS.getKey())) &&
                StringUtils.isNotBlank(newTopic.getSpec().getConfigs().get(RETENTION_BYTES_CONFIG))) {
            long used = getCurrentDiskTopicsByNamespace(namespace);
            long limit = BytesUtils.humanReadableToBytes(resourceQuota.getSpec().get(DISK_TOPICS.getKey()));

            long newTopicSize = Long.parseLong(newTopic.getSpec().getConfigs().get(RETENTION_BYTES_CONFIG)) * newTopic.getSpec().getPartitions();
            long existingTopicSize = existingTopic
                    .map(value -> Long.parseLong(value.getSpec().getConfigs().getOrDefault(RETENTION_BYTES_CONFIG, "0"))
                            * value.getSpec().getPartitions())
                    .orElse(0L);

            long bytesToAdd = newTopicSize - existingTopicSize;
            if (bytesToAdd > 0 && used + bytesToAdd > limit) {
                errors.add(String.format("Exceeding quota for %s: %s/%s (used/limit). Cannot add %s of data.", DISK_TOPICS,
                        BytesUtils.bytesToHumanReadable(used), BytesUtils.bytesToHumanReadable(limit), BytesUtils.bytesToHumanReadable(bytesToAdd)));
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
            long used = getCurrentCountConnectorsByNamespace(namespace);
            long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_CONNECTORS.getKey()));
            if (used + 1 > limit) {
                errors.add(String.format("Exceeding quota for %s: %s/%s (used/limit). Cannot add 1 connector.", COUNT_CONNECTORS, used, limit));
            }
        }

        return errors;
    }

    /**
     * Get the current consumed resources against the current quota of the given namespace to a response
     * @return A list of quotas as response format
     */
    public List<ResourceQuotaResponse> getUsedResourcesByQuotaForAllNamespaces() {
        return namespaceService.listAll()
                .stream()
                .map(namespace -> getUsedResourcesByQuotaByNamespace(namespace, findByNamespace(namespace.getMetadata().getName())))
                .collect(Collectors.toList());
    }

    /**
     * Map current consumed resources and current quota of the given namespace to a response
     * @param namespace The namespace
     * @param resourceQuota The quota to map
     * @return A list of quotas as response format
     */
    public ResourceQuotaResponse getUsedResourcesByQuotaByNamespace(Namespace namespace, Optional<ResourceQuota> resourceQuota) {
        long currentCountTopic = getCurrentCountTopicsByNamespace(namespace);
        long currentCountPartition = getCurrentCountPartitionsByNamespace(namespace);
        long currentDiskTopic = getCurrentDiskTopicsByNamespace(namespace);
        long currentCountConnector = getCurrentCountConnectorsByNamespace(namespace);

        return formatUsedResourceByQuotaResponse(currentCountTopic, currentCountPartition, currentDiskTopic,
                currentCountConnector, resourceQuota);
    }

    /**
     * Map given consumed resources and current quota to a response
     * @param currentCountTopic The current number of topics
     * @param currentCountPartition The current number of partitions
     * @param currentDiskTopic The current number of disk space used by topics
     * @param currentCountConnector The current number of connectors
     * @param resourceQuota The quota to map
     * @return A list of quotas as response format
     */
    public ResourceQuotaResponse formatUsedResourceByQuotaResponse(long currentCountTopic, long currentCountPartition, long currentDiskTopic,
                                                     long currentCountConnector, Optional<ResourceQuota> resourceQuota) {
        String countTopic = resourceQuota.isPresent() && StringUtils.isNotBlank(resourceQuota.get().getSpec().get(COUNT_TOPICS.getKey())) ?
                String.format(QUOTA_RESPONSE_FORMAT, currentCountTopic, resourceQuota.get().getSpec().get(COUNT_TOPICS.getKey())) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, currentCountTopic);

        String countPartition = resourceQuota.isPresent() && StringUtils.isNotBlank(resourceQuota.get().getSpec().get(COUNT_PARTITIONS.getKey())) ?
                String.format(QUOTA_RESPONSE_FORMAT, currentCountPartition, resourceQuota.get().getSpec().get(COUNT_PARTITIONS.getKey())) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, currentCountPartition);

        String diskTopic = resourceQuota.isPresent() && StringUtils.isNotBlank(resourceQuota.get().getSpec().get(DISK_TOPICS.getKey())) ?
                String.format(QUOTA_RESPONSE_FORMAT, BytesUtils.bytesToHumanReadable(currentDiskTopic), resourceQuota.get().getSpec().get(DISK_TOPICS.getKey())) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, BytesUtils.bytesToHumanReadable(currentDiskTopic));

        String countConnector = resourceQuota.isPresent() && StringUtils.isNotBlank(resourceQuota.get().getSpec().get(COUNT_CONNECTORS.getKey())) ?
                String.format(QUOTA_RESPONSE_FORMAT, currentCountConnector, resourceQuota.get().getSpec().get(COUNT_CONNECTORS.getKey())) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, currentCountConnector);

        return ResourceQuotaResponse.builder()
                .metadata(resourceQuota.map(ResourceQuota::getMetadata).orElse(null))
                .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                        .countTopic(countTopic)
                        .countPartition(countPartition)
                        .diskTopic(diskTopic)
                        .countConnector(countConnector)
                        .build())
                .build();
    }
}
