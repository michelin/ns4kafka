package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

@Singleton
public class SynchronizeNamespaceService {

    private static final Logger LOG = LoggerFactory.getLogger(SynchronizeNamespaceService.class);
    
    private Admin adminClient;

    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    public SynchronizeNamespaceService(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) throws MalformedURLException {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    private Admin getAdminClient(){
        if(this.adminClient==null){
            this.adminClient = Admin.create(kafkaAsyncExecutorConfig.getConfig());
        }
        return this.adminClient;
    }

    public List<Topic> buildTopics(List<String> topics, String namespace) throws InterruptedException, ExecutionException, TimeoutException {

        Map<String, TopicDescription> topicDescriptions = getAdminClient().describeTopics(topics).all().get();
        return getAdminClient()
                .describeConfigs(topics.stream()
                        .map(s -> new ConfigResource(ConfigResource.Type.TOPIC, s))
                        .collect(Collectors.toList())
                )
                //.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC,"*")))
                .all()
                .get(30, TimeUnit.SECONDS)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        configResourceConfigEntry -> configResourceConfigEntry.getKey().name()
                        , configResourceConfigEntry ->
                                configResourceConfigEntry.getValue().entries()
                                        .stream()
                                        .filter( configEntry -> configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))
                ))
                .entrySet()
                .stream()
                .map(stringMapEntry -> Topic.builder()
                        .metadata(ObjectMeta.builder()
                                .cluster(kafkaAsyncExecutorConfig.getName())
                                .name(stringMapEntry.getKey())
                                .namespace(namespace)
                                .build())
                        .spec(Topic.TopicSpec.builder()
                                .replicationFactor(topicDescriptions.get(stringMapEntry.getKey()).partitions().get(0).replicas().size())
                                .partitions(topicDescriptions.get(stringMapEntry.getKey()).partitions().size())
                                .configs(stringMapEntry.getValue())
                                .build())
                        .build()
                )
                .collect(Collectors.toList());
       }
    
    
    
}

