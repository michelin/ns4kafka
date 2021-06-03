package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Singleton
public class TopicService {
    @Inject
    TopicRepository topicRepository;
    @Inject
    AccessControlEntryService accessControlEntryService;
    @Inject
    ApplicationContext applicationContext;

    public Optional<Topic> findByName(Namespace namespace, String topic) {
        return findAllForNamespace(namespace)
            .stream()
            .filter(t -> t.getMetadata().getName().equals(topic))
            .findFirst();
    }

    public List<Topic> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace);
        return topicRepository.findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(topic -> acls.stream().anyMatch(accessControlEntry -> {
                //no need to check accessControlEntry.Permission, we want READ, WRITE or OWNER
                if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC) {
                    switch (accessControlEntry.getSpec().getResourcePatternType()) {
                    case PREFIXED:
                        return topic.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                    case LITERAL:
                        return topic.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                    }
                }
                return false;
            }))
            .collect(Collectors.toList());
    }

    public boolean isNamespaceOwnerOfTopic(String namespace, String topic) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.TOPIC, topic);
    }

    public Topic create(Topic topic) {
        return topicRepository.create(topic);
    }

    public void delete(Topic topic) {
        //TODO cleaner delete implementation, to be discussed
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(topic.getMetadata().getCluster()));
        try {
            topicAsyncExecutor.deleteTopic(topic);
        } catch (Exception e) {
            //TODO refactor global error handling model
            throw new ResourceValidationException(List.of(e.getMessage()));
        }
        topicRepository.delete(topic);
    }

    public void empty(Namespace namespace, String topic) throws InterruptedException, ExecutionException {

        Admin adminClient = KafkaHelper.createAdminClientFromNamespace(namespace);

        Map<TopicPartition, OffsetSpec> topicPartitionsOffsetSpec = new HashMap<>();
        adminClient.describeTopics(List.of(topic)).all().get().get(topic).partitions().forEach( partition -> {
                topicPartitionsOffsetSpec.put(new TopicPartition(topic, partition.partition()), OffsetSpec.latest() );
        });

        Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
        adminClient.listOffsets(topicPartitionsOffsetSpec).all().get().forEach((topicPartition, offsetResultInfo)-> {
                // add 1 to remove every records
                recordsToDelete.put(topicPartition, RecordsToDelete.beforeOffset(offsetResultInfo.offset() + 1));
        });

        adminClient.deleteRecords(recordsToDelete).all().get();

    }

    public static class KafkaHelper {

        @Inject
        private static List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

        public static Admin createAdminClientFromNamespace(Namespace namespace) {
            String cluster = namespace.getMetadata().getCluster();

            Optional<KafkaAsyncExecutorConfig> configOptional = kafkaAsyncExecutorConfigs.stream()
                .filter(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(cluster))
                .findFirst();
            Properties config = configOptional.get().getConfig();

            return KafkaAdminClient.create(config);
        }

    }
}
