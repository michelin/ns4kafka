package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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

    public Optional<Topic> findCollidingTopics(Topic topic)  {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(topic.getMetadata().getCluster()));
        try {
            Map<String, Topic> map = topicAsyncExecutor.collectBrokerTopics();
            return map.entrySet().stream()
                    .filter(
                            (entry ->
                                    topic.getMetadata().getName().equals(entry.getKey())
                                            && topic.getMetadata().getName().replace('.', '_')
                                            .equals(entry.getKey().replace('.', '_')))


                    ).map(
                            entry -> entry.getValue()
                    )
                    .findFirst();
        } catch (Exception e) {
            //TODO refactor global error handling model
            throw new ResourceValidationException(List.of(e.getMessage()));
        }
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

    public List<Topic> listUnsynchronizedTopics(Namespace namespace) throws ExecutionException, InterruptedException, TimeoutException {

        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        // List topics for this namespace
        List<String> topicNames = listUnsynchronizedTopicNames(namespace);
        // Get topics definitions
        Collection<Topic> unsynchronizedTopics = topicAsyncExecutor.collectBrokerTopicsFromNames(topicNames)
                .values();
        return new ArrayList<>(unsynchronizedTopics);
    }

    public List<String> listUnsynchronizedTopicNames(Namespace namespace) throws ExecutionException, InterruptedException, TimeoutException {

        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));
        // Get existing cluster topics...
        List<String> unsynchronizedTopicNames = topicAsyncExecutor.listBrokerTopicNames()
                .stream()
                // ...that belongs to this namespace
                .filter(topic -> isNamespaceOwnerOfTopic(namespace.getMetadata().getName(), topic))
                // ...and aren't in ns4kafka storage
                .filter(topic -> findByName(namespace, topic).isEmpty())
                .collect(Collectors.toList());
        return unsynchronizedTopicNames;
    }

    public Map<TopicPartition, Long> prepareRecordsToDelete(Topic topic) {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(topic.getMetadata().getCluster()));
        try {
            return topicAsyncExecutor.prepareRecordsToDelete(topic.getMetadata().getName())
                    .entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, kv -> kv.getValue().beforeOffset()));
        } catch (ExecutionException e) {
            //TODO refactor global error handling model
            throw new ResourceValidationException(List.of(e.getMessage()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        throw new ResourceValidationException(List.of("Unknown error"));
    }

    public Map<TopicPartition, Long> deleteRecords(Topic topic, Map<TopicPartition, Long> recordsToDelete) {

        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(topic.getMetadata().getCluster()));
        try {
            Map<TopicPartition, RecordsToDelete> recordsToDeleteMap = recordsToDelete.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, kv -> RecordsToDelete.beforeOffset(kv.getValue())));

            return topicAsyncExecutor.deleteRecords(recordsToDeleteMap);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            //TODO refactor global error handling model
            throw new ResourceValidationException(List.of(e.getMessage()));
        }
        throw new ResourceValidationException(List.of("Unknown error"));
    }

}
