package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;
import io.micronaut.context.annotation.EachBean;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class TopicAsyncExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAsyncExecutorScheduler.class);
    private Admin adminClient;
    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @Inject
    TopicRepository topicRepository;


    public TopicAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) throws MalformedURLException {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    private Admin getAdminClient(){
        if(this.adminClient==null){
            this.adminClient = Admin.create(kafkaAsyncExecutorConfig.getConfig());
        }
        return this.adminClient;
    }

    //TODO abstract synchronization process to handle different Kafka "models"
    // ie : cloud API vs AdminClient
    public void run(){

        // execute topic changes
        if(this.kafkaAsyncExecutorConfig.isManageTopics()) {
            synchronizeTopics();
        }

        if(this.kafkaAsyncExecutorConfig.isManageUsers()) {
            // TODO User + Password requires AdminClient and Brokers >= 2.7.0
            //  https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API
            //  Until then create the user/password without ns4kafka
            throw new UnsupportedOperationException("Not implemented, contributions welcome");
        }

    }
    /**** TOPICS MANAGEMENT ***/
    public void synchronizeTopics(){
        LOG.debug("Starting topic collection for cluster "+kafkaAsyncExecutorConfig.getName());
        try {
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
                    .filter(Objects::nonNull) //TODO can we avoid this filter ?
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if(LOG.isDebugEnabled()){
                LOG.debug("Topics to create : "+ toCreate.stream().map(t -> t.getMetadata().getName()).collect(Collectors.joining(", ")));
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

        } catch (ExecutionException | TimeoutException | CancellationException | KafkaStoreException e) {
            LOG.error("Error", e);
        } catch (InterruptedException e) {
            LOG.error("Error", e);
            Thread.currentThread().interrupt();
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
    public void deleteTopic(Topic topic) throws InterruptedException, ExecutionException, TimeoutException {
        getAdminClient().deleteTopics(List.of(topic.getMetadata().getName())).all().get(30, TimeUnit.SECONDS);
    }
    private void alterTopics(Map<ConfigResource, Collection<AlterConfigOp>> toUpdate, List<Topic> topics) {
        AlterConfigsResult alterConfigsResult = getAdminClient().incrementalAlterConfigs(toUpdate);
        alterConfigsResult.values().entrySet()
                .stream()
                .forEach(mapEntry -> {
                    Topic updatedTopic = topics.stream().filter(t -> t.getMetadata().getName().equals(mapEntry.getKey().name())).findFirst().get();
                    try {
                        mapEntry.getValue().get(10, TimeUnit.SECONDS);
                        Collection<AlterConfigOp> ops = toUpdate.get(mapEntry.getKey());
                        updatedTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                        updatedTopic.getMetadata().setGeneration(updatedTopic.getMetadata().getGeneration()+1);
                        updatedTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic configs updated"));
                        LOG.info(String.format("Success updating topic configs %s on %s : [%s]",
                                mapEntry.getKey().name(),
                                this.kafkaAsyncExecutorConfig.getName(),
                                ops.stream().map(alterConfigOp -> alterConfigOp.toString()).collect(Collectors.joining(","))));
                    } catch (InterruptedException e) {
                        LOG.error("Error", e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e){
                        updatedTopic.setStatus(Topic.TopicStatus.ofFailed("Error while updating topic configs: "+e.getMessage()));
                        LOG.error(String.format("Error while updating topic configs %s on %s", mapEntry.getKey().name(),this.kafkaAsyncExecutorConfig.getName()), e);
                    }
                    topicRepository.create(updatedTopic);
                });
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
                .forEach(mapEntry -> {
                    Topic createdTopic = topics.stream().filter(t -> t.getMetadata().getName().equals(mapEntry.getKey())).findFirst().get();
                    try {
                        mapEntry.getValue().get(10, TimeUnit.SECONDS);
                        createdTopic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                        createdTopic.getMetadata().setGeneration(1);
                        createdTopic.setStatus(Topic.TopicStatus.ofSuccess("Topic created"));
                        LOG.info(String.format("Success creating topic %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()));
                    } catch (InterruptedException e) {
                        LOG.error("Error", e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        createdTopic.setStatus(Topic.TopicStatus.ofFailed("Error while creating topic: "+e.getMessage()));
                        LOG.error(String.format("Error while creating topic %s on %s", mapEntry.getKey(),this.kafkaAsyncExecutorConfig.getName()), e);
                    }
                    topicRepository.create(createdTopic);
                });
    }
    private Map<String, Topic> collectBrokerTopicList() throws InterruptedException, ExecutionException, TimeoutException {
        List<String> topicNames = getAdminClient().listTopics().listings()
                .get(30, TimeUnit.SECONDS)
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
