package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.model.quota.ResourceQuota.ResourceQuotaSpecKey.COUNT_CONNECTORS;
import static com.michelin.ns4kafka.model.quota.ResourceQuota.ResourceQuotaSpecKey.COUNT_PARTITIONS;
import static com.michelin.ns4kafka.model.quota.ResourceQuota.ResourceQuotaSpecKey.COUNT_TOPICS;
import static com.michelin.ns4kafka.model.quota.ResourceQuota.ResourceQuotaSpecKey.DISK_TOPICS;
import static com.michelin.ns4kafka.model.quota.ResourceQuota.ResourceQuotaSpecKey.USER_CONSUMER_BYTE_RATE;
import static com.michelin.ns4kafka.model.quota.ResourceQuota.ResourceQuotaSpecKey.USER_PRODUCER_BYTE_RATE;
import static com.michelin.ns4kafka.util.BytesUtils.BYTE;
import static com.michelin.ns4kafka.util.BytesUtils.GIBIBYTE;
import static com.michelin.ns4kafka.util.BytesUtils.KIBIBYTE;
import static com.michelin.ns4kafka.util.BytesUtils.MEBIBYTE;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidFieldValidationNumber;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidQuotaAlreadyExceeded;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidQuotaFormat;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidQuotaOperation;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidQuotaOperationCannotAdd;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.model.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.repository.ResourceQuotaRepository;
import com.michelin.ns4kafka.service.executor.UserAsyncExecutor;
import com.michelin.ns4kafka.util.BytesUtils;
import com.michelin.ns4kafka.util.RegexUtils;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Service to manage resource quotas.
 */
@Slf4j
@Singleton
public class ResourceQuotaService {
    private static final String QUOTA_RESPONSE_FORMAT = "%s/%s";
    private static final String USER_QUOTA_RESPONSE_FORMAT = "%sB/s";

    private static final String NO_QUOTA_RESPONSE_FORMAT = "%s";

    @Inject
    ResourceQuotaRepository resourceQuotaRepository;

    @Inject
    TopicService topicService;

    @Inject
    ConnectorService connectorService;

    /**
     * Find a resource quota of a given namespace.
     *
     * @param namespace The namespace used to research
     * @return The researched resource quota
     */
    public Optional<ResourceQuota> findForNamespace(String namespace) {
        return resourceQuotaRepository.findForNamespace(namespace);
    }

    /**
     * Find a resource quota of a given namespace, filtered by name.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @return The researched resource quota
     */
    public List<ResourceQuota> findByWildcardName(String namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findForNamespace(namespace)
            .stream()
            .filter(quota -> RegexUtils.isResourceCoveredByRegex(quota.getMetadata().getName(), nameFilterPatterns))
            .toList();
    }

    /**
     * Find a resource quota by namespace and name.
     *
     * @param namespace The namespace
     * @param quota     The quota name
     * @return The researched resource quota
     */
    public Optional<ResourceQuota> findByName(String namespace, String quota) {
        return findForNamespace(namespace)
            .stream()
            .filter(resourceQuota -> resourceQuota.getMetadata().getName().equals(quota))
            .findFirst();
    }

    /**
     * Create a resource quota.
     *
     * @param resourceQuota The resource quota to create
     * @return The created resource quota
     */
    public ResourceQuota create(ResourceQuota resourceQuota) {
        return resourceQuotaRepository.create(resourceQuota);
    }

    /**
     * Delete a resource quota.
     *
     * @param resourceQuota The resource quota to delete
     */
    public void delete(ResourceQuota resourceQuota) {
        resourceQuotaRepository.delete(resourceQuota);
    }

    /**
     * Validate a given new resource quota against the current resource used by the namespace.
     *
     * @param namespace     The namespace
     * @param resourceQuota The new resource quota
     * @return A list of validation errors
     */
    public List<String> validateNewResourceQuota(Namespace namespace, ResourceQuota resourceQuota) {
        List<String> errors = new ArrayList<>();

        if (StringUtils.hasText(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()))) {
            long used = getCurrentCountTopicsByNamespace(namespace);
            long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()));
            if (used > limit) {
                errors.add(
                    invalidQuotaAlreadyExceeded(COUNT_TOPICS, String.valueOf(limit), String.valueOf(used)));
            }
        }

        if (StringUtils.hasText(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()))) {
            long used = getCurrentCountPartitionsByNamespace(namespace);
            long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()));
            if (used > limit) {
                errors.add(invalidQuotaAlreadyExceeded(COUNT_PARTITIONS, String.valueOf(limit),
                    String.valueOf(used)));
            }
        }

        if (StringUtils.hasText(resourceQuota.getSpec().get(DISK_TOPICS.getKey()))) {
            String limitAsString = resourceQuota.getSpec().get(DISK_TOPICS.getKey());
            if (!limitAsString.endsWith(BYTE) && !limitAsString.endsWith(KIBIBYTE)
                && !limitAsString.endsWith(MEBIBYTE) && !limitAsString.endsWith(GIBIBYTE)) {
                errors.add(invalidQuotaFormat(DISK_TOPICS, limitAsString));
            } else {
                long used = getCurrentDiskTopicsByNamespace(namespace);
                long limit = BytesUtils.humanReadableToBytes(limitAsString);
                if (used > limit) {
                    errors.add(
                        invalidQuotaAlreadyExceeded(DISK_TOPICS, limitAsString,
                            BytesUtils.bytesToHumanReadable(used)));
                }
            }
        }

        if (StringUtils.hasText(resourceQuota.getSpec().get(COUNT_CONNECTORS.getKey()))) {
            long used = getCurrentCountConnectorsByNamespace(namespace);
            long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_CONNECTORS.getKey()));
            if (used > limit) {
                errors.add(invalidQuotaAlreadyExceeded(COUNT_CONNECTORS, String.valueOf(limit),
                    String.valueOf(used)));
            }
        }

        String producerByteRate = resourceQuota.getSpec().get(USER_PRODUCER_BYTE_RATE.getKey());
        if (StringUtils.hasText(producerByteRate)) {
            try {
                Double.parseDouble(producerByteRate);
            } catch (NumberFormatException e) {
                errors.add(invalidFieldValidationNumber(USER_PRODUCER_BYTE_RATE.toString(), producerByteRate));
            }
        }

        String consumerByteRate = resourceQuota.getSpec().get(USER_CONSUMER_BYTE_RATE.getKey());
        if (StringUtils.hasText(consumerByteRate)) {
            try {
                Double.parseDouble(consumerByteRate);
            } catch (NumberFormatException e) {
                errors.add(invalidFieldValidationNumber(USER_CONSUMER_BYTE_RATE.toString(), consumerByteRate));
            }
        }

        return errors;
    }

    /**
     * Get currently used number of topics by namespace.
     *
     * @param namespace The namespace
     * @return The number of topics
     */
    public long getCurrentCountTopicsByNamespace(Namespace namespace) {
        return topicService.findAllForNamespace(namespace).size();
    }

    /**
     * Get currently used number of partitions by namespace.
     *
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
     * Get currently used topic disk in bytes by namespace.
     *
     * @param namespace The namespace
     * @return The number of topic disk
     */
    public long getCurrentDiskTopicsByNamespace(Namespace namespace) {
        return topicService.findAllForNamespace(namespace)
            .stream()
            .map(topic -> Long.parseLong(topic.getSpec().getConfigs().getOrDefault("retention.bytes", "0"))
                * topic.getSpec().getPartitions())
            .reduce(0L, Long::sum);
    }

    /**
     * Get currently used number of connectors by namespace.
     *
     * @param namespace The namespace
     * @return The number of connectors
     */
    public long getCurrentCountConnectorsByNamespace(Namespace namespace) {
        return connectorService.findAllForNamespace(namespace).size();
    }

    /**
     * Validate the topic quota.
     *
     * @param namespace     The namespace
     * @param existingTopic The existing topic
     * @param newTopic      The new topic
     * @return A list of errors
     */
    public List<String> validateTopicQuota(Namespace namespace, Optional<Topic> existingTopic, Topic newTopic) {
        Optional<ResourceQuota> resourceQuotaOptional = findForNamespace(namespace.getMetadata().getName());
        if (resourceQuotaOptional.isEmpty()) {
            return List.of();
        }

        List<String> errors = new ArrayList<>();
        ResourceQuota resourceQuota = resourceQuotaOptional.get();

        // Check count topics and count partitions only at creation
        if (existingTopic.isEmpty()) {
            if (StringUtils.hasText(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()))) {
                long used = getCurrentCountTopicsByNamespace(namespace);
                long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_TOPICS.getKey()));
                if (used + 1 > limit) {
                    errors.add(invalidQuotaOperation(COUNT_TOPICS, used, limit));
                }
            }

            if (StringUtils.hasText(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()))) {
                long used = getCurrentCountPartitionsByNamespace(namespace);
                long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_PARTITIONS.getKey()));
                if (used + newTopic.getSpec().getPartitions() > limit) {
                    errors.add(invalidQuotaOperationCannotAdd(COUNT_PARTITIONS, String.valueOf(used),
                        String.valueOf(limit), String.valueOf(newTopic.getSpec().getPartitions())));
                }
            }
        }

        if (StringUtils.hasText(resourceQuota.getSpec().get(DISK_TOPICS.getKey()))
            && StringUtils.hasText(newTopic.getSpec().getConfigs().get(RETENTION_BYTES_CONFIG))) {
            long used = getCurrentDiskTopicsByNamespace(namespace);
            long limit = BytesUtils.humanReadableToBytes(resourceQuota.getSpec().get(DISK_TOPICS.getKey()));

            long newTopicSize = Long.parseLong(newTopic.getSpec().getConfigs().get(RETENTION_BYTES_CONFIG))
                * newTopic.getSpec().getPartitions();
            long existingTopicSize = existingTopic
                .map(value -> Long.parseLong(value.getSpec().getConfigs().getOrDefault(RETENTION_BYTES_CONFIG, "0"))
                    * value.getSpec().getPartitions())
                .orElse(0L);

            long bytesToAdd = newTopicSize - existingTopicSize;
            if (bytesToAdd > 0 && used + bytesToAdd > limit) {
                errors.add(invalidQuotaOperationCannotAdd(DISK_TOPICS, BytesUtils.bytesToHumanReadable(used),
                    BytesUtils.bytesToHumanReadable(limit), BytesUtils.bytesToHumanReadable(bytesToAdd)));
            }
        }

        return errors;
    }

    /**
     * Validate the connector quota.
     *
     * @param namespace The namespace
     * @return A list of errors
     */
    public List<String> validateConnectorQuota(Namespace namespace) {
        Optional<ResourceQuota> resourceQuotaOptional = findForNamespace(namespace.getMetadata().getName());
        if (resourceQuotaOptional.isEmpty()) {
            return List.of();
        }

        List<String> errors = new ArrayList<>();
        ResourceQuota resourceQuota = resourceQuotaOptional.get();

        if (StringUtils.hasText(resourceQuota.getSpec().get(COUNT_CONNECTORS.getKey()))) {
            long used = getCurrentCountConnectorsByNamespace(namespace);
            long limit = Long.parseLong(resourceQuota.getSpec().get(COUNT_CONNECTORS.getKey()));
            if (used + 1 > limit) {
                errors.add(invalidQuotaOperation(COUNT_CONNECTORS, used, limit));
            }
        }

        return errors;
    }

    /**
     * Get the current consumed resources against the current quota of the given namespace to a response.
     *
     * @return A list of quotas as response format
     */
    public List<ResourceQuotaResponse> getUsedQuotaByNamespaces(List<Namespace> namespaces) {
        return namespaces
            .stream()
            .map(namespace -> getUsedResourcesByQuotaByNamespace(namespace,
                findForNamespace(namespace.getMetadata().getName())))
            .toList();
    }

    /**
     * Map current consumed resources and current quota of the given namespace to a response.
     *
     * @param namespace     The namespace
     * @param resourceQuota The quota to map
     * @return A list of quotas as response format
     */
    public ResourceQuotaResponse getUsedResourcesByQuotaByNamespace(Namespace namespace,
                                                                    Optional<ResourceQuota> resourceQuota) {
        long currentCountTopic = getCurrentCountTopicsByNamespace(namespace);
        long currentCountPartition = getCurrentCountPartitionsByNamespace(namespace);
        long currentDiskTopic = getCurrentDiskTopicsByNamespace(namespace);
        long currentCountConnector = getCurrentCountConnectorsByNamespace(namespace);

        return formatUsedResourceByQuotaResponse(namespace, currentCountTopic, currentCountPartition, currentDiskTopic,
            currentCountConnector, resourceQuota);
    }

    /**
     * Map given consumed resources and current quota to a response.
     *
     * @param namespace             The namespace
     * @param currentCountTopic     The current number of topics
     * @param currentCountPartition The current number of partitions
     * @param currentDiskTopic      The current number of disk space used by topics
     * @param currentCountConnector The current number of connectors
     * @param resourceQuota         The quota to map
     * @return A list of quotas as response format
     */
    public ResourceQuotaResponse formatUsedResourceByQuotaResponse(Namespace namespace, long currentCountTopic,
                                                                   long currentCountPartition, long currentDiskTopic,
                                                                   long currentCountConnector,
                                                                   Optional<ResourceQuota> resourceQuota) {
        String countTopic =
            resourceQuota.isPresent() && StringUtils.hasText(resourceQuota.get().getSpec().get(COUNT_TOPICS.getKey()))
                ? String.format(QUOTA_RESPONSE_FORMAT, currentCountTopic,
                resourceQuota.get().getSpec().get(COUNT_TOPICS.getKey())) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, currentCountTopic);

        String countPartition = resourceQuota.isPresent()
            && StringUtils.hasText(resourceQuota.get().getSpec().get(COUNT_PARTITIONS.getKey()))
            ? String.format(QUOTA_RESPONSE_FORMAT, currentCountPartition,
            resourceQuota.get().getSpec().get(COUNT_PARTITIONS.getKey())) :
            String.format(NO_QUOTA_RESPONSE_FORMAT, currentCountPartition);

        String diskTopic =
            resourceQuota.isPresent() && StringUtils.hasText(resourceQuota.get().getSpec().get(DISK_TOPICS.getKey()))
                ? String.format(QUOTA_RESPONSE_FORMAT, BytesUtils.bytesToHumanReadable(currentDiskTopic),
                resourceQuota.get().getSpec().get(DISK_TOPICS.getKey())) :
                String.format(NO_QUOTA_RESPONSE_FORMAT, BytesUtils.bytesToHumanReadable(currentDiskTopic));

        String countConnector = resourceQuota.isPresent()
            && StringUtils.hasText(resourceQuota.get().getSpec().get(COUNT_CONNECTORS.getKey()))
            ? String.format(QUOTA_RESPONSE_FORMAT, currentCountConnector,
            resourceQuota.get().getSpec().get(COUNT_CONNECTORS.getKey())) :
            String.format(NO_QUOTA_RESPONSE_FORMAT, currentCountConnector);

        String consumerByteRate = resourceQuota.isPresent()
            && StringUtils.hasText(resourceQuota.get().getSpec().get(USER_CONSUMER_BYTE_RATE.getKey()))
            ? String.format(USER_QUOTA_RESPONSE_FORMAT,
            resourceQuota.get().getSpec().get(USER_CONSUMER_BYTE_RATE.getKey())) :
            String.format(USER_QUOTA_RESPONSE_FORMAT, UserAsyncExecutor.BYTE_RATE_DEFAULT_VALUE);

        String producerByteRate = resourceQuota.isPresent()
            && StringUtils.hasText(resourceQuota.get().getSpec().get(USER_PRODUCER_BYTE_RATE.getKey()))
            ? String.format(USER_QUOTA_RESPONSE_FORMAT,
            resourceQuota.get().getSpec().get(USER_PRODUCER_BYTE_RATE.getKey())) :
            String.format(USER_QUOTA_RESPONSE_FORMAT, UserAsyncExecutor.BYTE_RATE_DEFAULT_VALUE);

        return ResourceQuotaResponse.builder()
            .metadata(resourceQuota.map(ResourceQuota::getMetadata).orElse(Metadata.builder()
                .namespace(namespace.getMetadata().getName())
                .cluster(namespace.getMetadata().getCluster())
                .build()))
            .spec(ResourceQuotaResponse.ResourceQuotaResponseSpec.builder()
                .countTopic(countTopic)
                .countPartition(countPartition)
                .diskTopic(diskTopic)
                .countConnector(countConnector)
                .consumerByteRate(consumerByteRate)
                .producerByteRate(producerByteRate)
                .build())
            .build();
    }
}
