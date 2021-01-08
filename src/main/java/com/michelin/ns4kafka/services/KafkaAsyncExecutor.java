package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.scheduling.annotation.Scheduled;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class KafkaAsyncExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAsyncExecutorScheduler.class);
    private Admin adminClient;
    private KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @Inject
    TopicRepository topicRepository;

    public KafkaAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig){
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    private Admin getAdminClient(){
        if(this.adminClient==null){
            this.adminClient = Admin.create(kafkaAsyncExecutorConfig.getConfig());
        }
        return this.adminClient;
    }

    public void run(){
        //execute topic changes
        synchronizeTopics();
        //execute user changes
    }
    public void synchronizeTopics(){
        LOG.debug("Starting topic collection for cluster "+kafkaAsyncExecutorConfig.getName());
        try {
            // Ready ?
            topicRepository.assertInitialized();
            // List topics from broker
            Map<String, Topic> brokerTopicList = collectBrokerTopicList();
            // List topics from ns4kafka Repository
            List<Topic> ns4kafkaTopicList = topicRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName());

            // Compute toCreate, toDelete, and toUpdate lists
            List<Topic> toCreate = ns4kafkaTopicList.stream()
                    .filter(topic -> !brokerTopicList.containsKey(topic.getMetadata().getName()))
                    .collect(Collectors.toList());

            List<Topic> toDelete = brokerTopicList.values()
                    .stream()
                    .filter(topic -> ns4kafkaTopicList.stream().noneMatch(topic1 -> topic1.getMetadata().getName().equals(topic.getMetadata().getName())))
                    .collect(Collectors.toList());

            List<Topic> toCheckConf = ns4kafkaTopicList.stream()
                    .filter(topic -> brokerTopicList.containsKey(topic.getMetadata().getName()))
                    .collect(Collectors.toList());
            Map<ConfigResource, Collection<AlterConfigOp>> toUpdate = toCheckConf.stream()
                    .map(topic -> {
                        Map<String,String> actualConf = brokerTopicList.get(topic.getMetadata().getName()).getSpec().getConfigs();
                        Map<String,String> expectedConf = topic.getSpec().getConfigs() == null ? Map.of() : topic.getSpec().getConfigs();
                        Collection<AlterConfigOp> topicConfigChanges = computeConfigChanges(expectedConf,actualConf);
                        if(topicConfigChanges.size()>0){
                            ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topic.getMetadata().getName());
                            return Map.entry(cr,topicConfigChanges);
                        }
                        return null;
                    })
                    .filter(mapEntry -> mapEntry != null) //TODO can we avoid this filter ?
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if(LOG.isDebugEnabled()){
                LOG.debug("Topics to create : "+String.join(", ", toCreate.stream().map(t -> t.getMetadata().getName()).collect(Collectors.toList())));
                //TODO reenable
                // LOG.debug("Topics to delete : "+String.join(", ", toDelete.stream().map(t -> t.getMetadata().getName()).collect(Collectors.toList())));
                LOG.debug("Topics to delete : "+toDelete.size());
                LOG.debug("Topic configs to update : "+toUpdate.size());
                for (Map.Entry<ConfigResource,Collection<AlterConfigOp>> e : toUpdate.entrySet()) {
                    for (AlterConfigOp op : e.getValue()) {
                        LOG.debug(e.getKey().name()+" "+op.opType().toString()+" " +op.configEntry().name()+"("+op.configEntry().value()+")");
                    }
                }
            }
            //creating topics
            createTopics(toCreate);
            //delete
            deleteTopics(toDelete);
            //alter
            alterTopics(toUpdate, toCheckConf);

        } catch (InterruptedException e) {
            LOG.error("Error", e);
        } catch (ExecutionException e) {
            LOG.error("Error", e);
        } catch (TimeoutException e) {
            LOG.error("Error", e);
        } catch (CancellationException e){
            LOG.error("Error", e);
        } catch (KafkaStoreException e){
            LOG.error("Error", e);
        }

    }

    private void deleteTopics(List<Topic> topics) {
        //TODO What's the best way to prevent delete __consumer_offsets and other internal topics ?
        // delete only topics that belongs to a namespace and ignore others ?
        // create a technical namespace with internal topics ? risky
        // delete synchronously from DELETE API calls ?
        // other ?
        /* 
        DeleteTopicsResult deleteTopicsResult = getAdminClient().deleteTopics(topics.stream().map(topic -> topic.getMetadata().getName()).collect(Collectors.toList()));
        deleteTopicsResult.values().entrySet()
                .stream()
                .forEach(mapEntry -> mapEntry.getValue()
                        .whenComplete((unused, throwable) ->{
                            Topic deleted = topics.stream().filter(t -> t.getMetadata().getName().equals(mapEntry.getKey())).findFirst().get();
                            if(throwable!=null){
                                LOG.error(String.format("Error while deleting topic %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()), throwable);
                            }else{
                                LOG.info(String.format("Success deleting topic %s on %s : [%s]", mapEntry.getKey(), this.kafkaAsyncExecutorConfig.getName()));
                            }
                        })
                );
         */
    }

    private void alterTopics(Map<ConfigResource, Collection<AlterConfigOp>> toUpdate, List<Topic> topics) {
        AlterConfigsResult alterConfigsResult = getAdminClient().incrementalAlterConfigs(toUpdate);
        alterConfigsResult.values().entrySet()
                .stream()
                .forEach(mapEntry -> mapEntry.getValue()
                        .whenComplete((unused, throwable) ->{
                            Topic updatedTopic = topics.stream().filter(t -> t.getMetadata().getName().equals(mapEntry.getKey().name())).findFirst().get();
                            if(throwable!=null){
                                updatedTopic.setStatus(Topic.TopicStatus.ofFailed("Error while updating topic configs: "+throwable.getMessage()));
                                LOG.error(String.format("Error while updating topic configs %s on %s", mapEntry.getKey().name(),this.kafkaAsyncExecutorConfig.getName()), throwable);
                            }else{
                                Collection<AlterConfigOp> ops = toUpdate.get(mapEntry.getKey());
                                updatedTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                                updatedTopic.getMetadata().setGeneration(updatedTopic.getMetadata().getGeneration()+1);
                                updatedTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic configs updated"));
                                LOG.info(String.format("Success updating topic configs %s on %s : [%s]",
                                        mapEntry.getKey().name(),
                                        this.kafkaAsyncExecutorConfig.getName(),
                                        String.join(",", ops.stream().map(alterConfigOp -> alterConfigOp.toString()).collect(Collectors.toList()))));
                            }
                            topicRepository.create(updatedTopic);
                        })
                );
    }

    private void createTopics(List<Topic> topics) {
        List<NewTopic> newTopics = topics.stream()
                .map(topic -> {
                    LOG.debug(String.format("Creating topic %s on %s",topic.getMetadata().getName(),topic.getMetadata().getCluster()));
                    NewTopic newTopic = new NewTopic(topic.getMetadata().getName(),topic.getSpec().getPartitions(), (short) topic.getSpec().getReplicationFactor());
                    newTopic.configs(topic.getSpec().getConfigs());
                    LOG.debug(newTopic.toString());
                    return newTopic;
                })
                .collect(Collectors.toList());
        CreateTopicsResult createTopicsResult = getAdminClient().createTopics(newTopics);
        createTopicsResult.values().entrySet()
                .stream()
                .forEach(mapEntry -> mapEntry.getValue()
                        .whenComplete((unused, throwable) ->{
                            Topic createdTopic = topics.stream().filter(t -> t.getMetadata().getName().equals(mapEntry.getKey())).findFirst().get();
                            if(throwable!=null){
                                createdTopic.setStatus(Topic.TopicStatus.ofFailed("Error while creating topic: "+throwable.getMessage()));
                                LOG.error(String.format("Error while creating topic %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()), throwable);
                            }else{
                                createdTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                                createdTopic.getMetadata().setGeneration(1);
                                createdTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic created"));
                                LOG.info(String.format("Success creating topic %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()));
                            }
                            topicRepository.create(createdTopic);
                        })
                );
    }

    private Map<String, Topic> collectBrokerTopicList() throws InterruptedException, ExecutionException, TimeoutException {
        List<String> topicNames = getAdminClient().listTopics().listings()
                .get(10, TimeUnit.SECONDS)
                .stream()
                .map(topicListing -> topicListing.name())
                .collect(Collectors.toList());
        Map<String, TopicDescription> topicDescriptions = getAdminClient().describeTopics(topicNames).all().get();
        // Create a Map<TopicName, Map<ConfigName, ConfigValue>> for all topics
        // includes only Dynamic config properties
        return getAdminClient()
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
                                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))
                ))
                .entrySet()
                .stream()
                .map(stringMapEntry -> Topic.builder()
                        .metadata(ObjectMeta.builder()
                                .cluster(kafkaAsyncExecutorConfig.getName())
                                .name(stringMapEntry.getKey())
                                .build())
                        .spec(Topic.TopicSpec.builder()
                                .replicationFactor(topicDescriptions.get(stringMapEntry.getKey()).partitions().get(0).replicas().size())
                                .partitions(topicDescriptions.get(stringMapEntry.getKey()).partitions().size())
                                .configs(stringMapEntry.getValue())
                                .build())
                        .build()
                )
                .collect(Collectors.toMap( topic -> topic.getMetadata().getName(), Function.identity()));
    }

    private void synchronizeUsers(){
        //
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
