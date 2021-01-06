package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.context.annotation.EachBean;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class KafkaAsyncExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAsyncExecutorScheduler.class);
    private AdminClient adminClient;
    private KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @Inject
    TopicRepository topicRepository;

    private Map<String, Topic> brokerTopicList;

    public KafkaAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig){
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    private AdminClient getAdminClient(){
        if(this.adminClient==null){
            this.adminClient = AdminClient.create(kafkaAsyncExecutorConfig.getConfig());
        }
        return this.adminClient;
    }


    public void collect(){
        //TODO ready ? sync OK ?
        LOG.debug("Starting collection for cluster "+kafkaAsyncExecutorConfig.getName());
        try {
            // List topics from broker
            List<String> topicNames = getAdminClient().listTopics().listings()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .map(topicListing -> topicListing.name())
                    .collect(Collectors.toList());
            Map<String, TopicDescription> topicDescriptions = getAdminClient().describeTopics(topicNames).all().get();
            // Create a Map<TopicName, Map<ConfigName, ConfigValue>> for all topics
            // includes only Dynamic config properties
            brokerTopicList = getAdminClient()
                    .describeConfigs(topicNames.stream()
                            .map(s -> new ConfigResource(ConfigResource.Type.TOPIC, s))
                            .collect(Collectors.toList())
                    )
                    .all()
                    .get()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            configResourceConfigEntry -> configResourceConfigEntry.getKey().name()
                            , configResourceConfigEntry ->
                                    configResourceConfigEntry.getValue().entries()
                                            .stream()
                                            .filter( configEntry -> configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                                            //.peek(configEntry -> LOG.debug(String.format("%s %s %s", configResourceConfigEntry.getKey().name(),
                                            //        configEntry.name(), configEntry.value())))
                                            .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))
                    ))
                    .entrySet()
                    .stream()
                    .map(stringMapEntry -> {
                        Topic t = new Topic();
                        t.setCluster(kafkaAsyncExecutorConfig.getName());
                        t.setName(stringMapEntry.getKey());
                        t.setConfig(stringMapEntry.getValue());
                        t.setPartitions(topicDescriptions.get(stringMapEntry.getKey()).partitions().size());
                        t.setReplicationFactor(topicDescriptions.get(stringMapEntry.getKey()).partitions().get(0).replicas().size());
                        return t;
                    })
                    .collect(Collectors.toMap(Topic::getName, Function.identity()));

            // List topics from ns4kafka Repository
            List<Topic> ns4kafkaTopicList = topicRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName());

            // Compute toCreate, toDelete, and toUpdate lists
            List<Topic> toCreate = ns4kafkaTopicList.stream()
                    .filter(topic -> !brokerTopicList.containsKey(topic.getName()))
                    .collect(Collectors.toList());

            List<Topic> toDelete = brokerTopicList.values()
                    .stream()
                    .filter(topic -> ns4kafkaTopicList.stream().noneMatch(topic1 -> topic1.getName().equals(topic.getName())))
                    .collect(Collectors.toList());

            List<Topic> toCheckConf = ns4kafkaTopicList.stream()
                    .filter(topic -> brokerTopicList.containsKey(topic.getName()))
                    .collect(Collectors.toList());
            LOG.debug("A Creer : "+toCreate.size());
            LOG.debug("A Delete : "+toDelete.size());
            LOG.debug("A Check : "+toCheckConf.size());

            //TODO not tested
            Map<ConfigResource, Collection<AlterConfigOp>> toUpdate = toCheckConf.stream().map(topic -> {
                Map<String,String> actualConf = brokerTopicList.get(topic.getName()).getConfig();
                Map<String,String> expectedConf = topic.getConfig() == null ? Map.of() : topic.getConfig();
                Collection<AlterConfigOp> topicConfigChanges = computeConfigChanges(expectedConf,actualConf);
                if(topicConfigChanges.size()>0){
                    ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topic.getName());
                    return Map.entry(cr,topicConfigChanges);
                }
                return null;
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            LOG.debug("After check, A Update : "+toUpdate.size());
            for (Map.Entry<ConfigResource,Collection<AlterConfigOp>> e :
                    toUpdate.entrySet()) {

                for (AlterConfigOp op :
                        e.getValue()) {
                    LOG.debug(e.getKey().name()+" "+op.opType().toString()+" " +op.configEntry().name()+"("+op.configEntry().value()+")");
                }
            }



        } catch (InterruptedException e) {
            LOG.error("Error", e);
        } catch (ExecutionException e) {
            LOG.error("Error", e);
        } catch (TimeoutException e) {
            LOG.error("Error", e);
        } catch (CancellationException e){
            LOG.error("Error", e);
        }

    }
    private Collection<AlterConfigOp> computeConfigChanges(Map<String,String> expected, Map<String,String> actual){
        List<AlterConfigOp> toCreate = expected.entrySet()
                .stream()
                .filter(expectedEntry -> !actual.containsKey(expectedEntry.getKey()))
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),expectedEntry.getValue()), AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());
        List<AlterConfigOp> toDelete = actual.entrySet()
                .stream()
                .filter(actualEntry -> !expected.containsKey(actualEntry.getKey()))
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),null), AlterConfigOp.OpType.DELETE))
                .collect(Collectors.toList());
        List<AlterConfigOp> toChange = expected.entrySet()
                .stream()
                .filter(expectedEntry -> {
                    if (actual.containsKey(expectedEntry.getKey())) {
                        String actualVal = actual.get(expectedEntry.getKey());
                        String expectedVal = expectedEntry.getValue();
                        return !expectedVal.equals(actualVal);
                    }
                    return false;
                })
                .map(expectedEntry -> new AlterConfigOp(new ConfigEntry(expectedEntry.getKey(),expectedEntry.getValue()), AlterConfigOp.OpType.SET))
                .collect(Collectors.toList());

        List<AlterConfigOp> total = new ArrayList<>();
        total.addAll(toCreate);
        total.addAll(toDelete);
        total.addAll(toChange);

        return total;
    }

}
