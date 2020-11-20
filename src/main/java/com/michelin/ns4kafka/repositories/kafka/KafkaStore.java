package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.Application;
import io.micronaut.configuration.kafka.ConsumerAware;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import io.micronaut.scheduling.TaskScheduler;
import io.micronaut.scheduling.annotation.Scheduled;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class KafkaStore<T> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStore.class);

    @Inject ApplicationContext applicationContext;
    @Inject AdminClient adminClient;


    Map<String,T> kafkaStore;
    String topic;
    Producer<String,T> kafkaProducer;
    long lastReadOffset = -1;
    long lastWrittenOffset = Long.MAX_VALUE;
    int initTimeout = 10000;
    int desiredReplicationFactor = 1;

    public KafkaStore(String topic, Producer<String,T> kafkaProducer){
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
        this.kafkaStore = new ConcurrentHashMap<String,T>();
    }

    T produce(String key, T message){
        try {
            long startTime = System.currentTimeMillis();

            RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<>(topic, key, message)).get(initTimeout, TimeUnit.MILLISECONDS);
            lastWrittenOffset = metadata.offset();
            LOG.debug("Produces message "+message.getClass().toString()+" at offset "+lastWrittenOffset+". Awaiting KaflaStore sync");
            // TODO use Lock await and signal
            // good enough for proof of concept
            int i = 0;
            while(i++ < 50) {
                if (lastReadOffset >= lastWrittenOffset) {
                    //great !
                    long delta = System.currentTimeMillis() - startTime;
                    LOG.debug("Synced object "+message.getClass().toString()+ " in ms:"+delta);
                    return kafkaStore.get(key);
                }else{
                    Thread.sleep(200);
                }
            }
            throw new TimeoutException("Timed out waiting for offset sync !");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        return kafkaStore.get(key);
    }

    void receive(ConsumerRecord<String, T> record) {
        if(record.key().equals("NOOP")){
            LOG.debug("NOOP record, doing nothing");
        }else if (record.value()==null){
            LOG.debug("DELETE REQUESTED");
            kafkaStore.remove(record.key());
        }else {
            LOG.debug("ADDED");
            kafkaStore.put(record.key(), record.value());
        }

        lastReadOffset = record.offset();
    }

    @PostConstruct
    private void createOrVerifyTopic() throws StoreInitializationException {
        createOrVerifySchemaTopic(topic);
        produceNOOP();
    }
    void produceNOOP() throws StoreInitializationException {
        try {
            LOG.trace("Sending Noop record to KafkaStore to find last offset.");
            Future<RecordMetadata> ack = kafkaProducer.send(new ProducerRecord<>(topic,"NOOP",null));
            RecordMetadata metadata = ack.get(initTimeout, TimeUnit.MILLISECONDS);
            this.lastWrittenOffset = metadata.offset();
            LOG.trace("Noop record's offset is " + this.lastWrittenOffset);
        } catch (Exception e) {
            throw new StoreInitializationException("Failed to write Noop record to kafka store.", e);
        }
    }

    @Scheduled(initialDelay = "10s" )
    void onceOneMinuteAfterStartup() {
        if(lastReadOffset>=lastWrittenOffset) {
            LOG.info("Target offset reached for topic "+topic);
        }else{
            LOG.error("Server stop requested because could not read fully topic "+topic);
            applicationContext.stop();
        }
    }

    // BEGIN http://www.confluent.io/confluent-community-license
    private void createOrVerifySchemaTopic(String topic) throws StoreInitializationException {

        try {
            Set<String> allTopics = adminClient.listTopics().names().get(initTimeout, TimeUnit.MILLISECONDS);
            if (allTopics.contains(topic)) {
                verifySchemaTopic(topic);
            } else {
                createSchemaTopic(topic);
            }
        } catch (TimeoutException e) {
            throw new StoreInitializationException(
                    "Timed out trying to create or validate topic configuration",
                    e
            );
        } catch (InterruptedException | ExecutionException e) {
            throw new StoreInitializationException(
                    "Failed trying to create or validate topic configuration",
                    e
            );
        }
    }

    private void createSchemaTopic(String topic) throws StoreInitializationException,
            InterruptedException,
            ExecutionException,
            TimeoutException {
        LOG.info("Creating topic {}", topic);

        int numLiveBrokers = adminClient.describeCluster().nodes()
                .get(initTimeout, TimeUnit.MILLISECONDS).size();
        if (numLiveBrokers <= 0) {
            throw new StoreInitializationException("No live Kafka brokers");
        }

        int schemaTopicReplicationFactor = Math.min(numLiveBrokers, desiredReplicationFactor);
        if (schemaTopicReplicationFactor < desiredReplicationFactor) {
            LOG.warn("Creating the topic "
                    + topic
                    + " using a replication factor of "
                    + schemaTopicReplicationFactor
                    + ", which is less than the desired one of "
                    + desiredReplicationFactor + ". If this is a production environment, it's "
                    + "crucial to add more brokers and increase the replication factor of the topic.");
        }

        NewTopic schemaTopicRequest = new NewTopic(topic, 1, (short) schemaTopicReplicationFactor);
        Map topicConfigs = new HashMap();
        topicConfigs.put(
                TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_COMPACT
        );
        //TODO handle ns4kafka.storage.kafka.topics.config
        schemaTopicRequest.configs(topicConfigs);
        try {
            adminClient.createTopics(Collections.singleton(schemaTopicRequest)).all()
                    .get(initTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                // If topic already exists, ensure that it is configured correctly.
                verifySchemaTopic(topic);
            } else {
                throw e;
            }
        }
    }

    private void verifySchemaTopic(String topic) throws StoreInitializationException,
            InterruptedException,
            ExecutionException,
            TimeoutException {
        LOG.info("Validating topic {}", topic);

        Set<String> topics = Collections.singleton(topic);
        Map<String, TopicDescription> topicDescription = adminClient.describeTopics(topics)
                .all().get(initTimeout, TimeUnit.MILLISECONDS);

        TopicDescription description = topicDescription.get(topic);
        final int numPartitions = description.partitions().size();
        if (numPartitions != 1) {
            throw new StoreInitializationException("The topic " + topic + " should have only 1 "
                    + "partition but has " + numPartitions);
        }

        if (description.partitions().get(0).replicas().size() < desiredReplicationFactor) {
            LOG.warn("The replication factor of the topic "
                    + topic
                    + " is less than the desired one of "
                    + desiredReplicationFactor
                    + ". If this is a production environment, it's crucial to add more brokers and "
                    + "increase the replication factor of the topic.");
        }

        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        Map<ConfigResource, Config> configs =
                adminClient.describeConfigs(Collections.singleton(topicResource)).all()
                        .get(initTimeout, TimeUnit.MILLISECONDS);
        Config topicConfigs = configs.get(topicResource);
        String retentionPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
        if (retentionPolicy == null || !TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
            LOG.error("The retention policy of the topic " + topic + " is incorrect. "
                    + "You must configure the topic to 'compact' cleanup policy to avoid Kafka "
                    + "deleting your data after a week. "
                    + "Refer to Kafka documentation for more details on cleanup policies");

            throw new StoreInitializationException("The retention policy of the schema topic " + topic
                    + " is incorrect. Expected cleanup.policy to be "
                    + "'compact' but it is " + retentionPolicy);

        }
    }
    // END http://www.confluent.io/confluent-community-license

}
