package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

    /**
     * @param literalTopics
     * @param namespace
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public Map<String, Topic> buildTopicNames(List<String> literalTopics, List<String> prefixedPattern, Namespace namespace) throws InterruptedException, ExecutionException, TimeoutException {

        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        // Get existing cluster topics
        List<String> clusterTopics = topicAsyncExecutor.listTopics()
                .stream()
                .collect(Collectors.toList());
        
        // filter prefixed topics
        List<String> topics = clusterTopics.stream()
                .filter(
                        topic -> {

                            if (StringUtils.startsWithAny(topic, prefixedPattern.toArray(new String[0]))) {
                                return true;
                            }
                            if(literalTopics.contains(topic)){
                                return true;
                            }
                            return false;
                        })
                .collect(Collectors.toList());
        
        return topicAsyncExecutor.collectBrokerTopicList(topics);

    }
    
}
