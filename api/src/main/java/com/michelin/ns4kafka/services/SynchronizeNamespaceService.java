package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.util.*;
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
    
    @Inject
    private KafkaConnectClient kafkaConnectClient;

    public SynchronizeNamespaceService(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) throws MalformedURLException {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    private Admin getAdminClient(){
        if(this.adminClient==null){
            this.adminClient = Admin.create(kafkaAsyncExecutorConfig.getConfig());
        }
        return this.adminClient;
    }

    /**
     * 
     * @param topics
     * @param namespace
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public List<Topic> buildTopics(List<String> topics, String namespace) throws InterruptedException, ExecutionException, TimeoutException {
        
        List<String> existingTopics =  topics.stream()
                .filter( topic -> {
                        try {
                            getAdminClient().describeTopics(Arrays.asList(topic)).all().get();
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
        }
        ).collect(Collectors.toList());
        
        Map<String, TopicDescription> topicDescriptions = getAdminClient().describeTopics(existingTopics).all().get();
        if(topicDescriptions.size() == 0){
            return new ArrayList<>();
        }
        return getAdminClient()
                .describeConfigs(existingTopics.stream()
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

    /**
     * 
     * @param patterns
     * @return
     */
    public List<Connector> buildConnectList(List<String> patterns){
           List<Connector> connectors = new ArrayList<>();
           String[] patternArray = new String[patterns.size()];
           patterns.toArray(patternArray);
           kafkaAsyncExecutorConfig.getConnects().forEach(
                   (connectCluster, connectConfig) ->
                           connectors.addAll(kafkaConnectClient.listAll(kafkaAsyncExecutorConfig.getName(), connectCluster)
                                   .values()
                                   .stream()
                                   .map(connectorStatus -> {
                                       return Connector.builder()
                                               .metadata(ObjectMeta.builder()
                                                       // Any other metadata is not usefull for this process
                                                       .name(connectorStatus.getInfo().name())
                                                       .cluster(kafkaAsyncExecutorConfig.getName())
                                                       .build())
                                               .spec(Connector.ConnectorSpec.builder()
                                                       .connectCluster(connectCluster)
                                                       .config(connectorStatus.getInfo().config())
                                                       .build())
                                               .build();

                                   })
                                   .filter(connector -> StringUtils.startsWithAny(
                                           connector.getMetadata().getName(), patternArray)
                                   )
                                   .collect(Collectors.toList()))
           );
           return connectors;
       }
       
}

