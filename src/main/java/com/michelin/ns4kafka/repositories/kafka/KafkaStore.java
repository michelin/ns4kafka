package com.michelin.ns4kafka.repositories.kafka;

import io.micronaut.context.ApplicationContext;
import io.micronaut.scheduling.annotation.Scheduled;
import org.apache.kafka.clients.admin.*;
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
import java.util.*;
import java.util.concurrent.*;

public abstract class KafkaStore<T> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStore.class);

    @Inject ApplicationContext applicationContext;
    @Inject AdminClient adminClient;

    @Inject KafkaStoreConfig kafkaStoreConfig;

    Map<String,T> kafkaStore;
    String kafkaTopic;
    Producer<String,T> kafkaProducer;
    long lastReadOffset = -1;
    long lastWrittenOffset = Long.MAX_VALUE;
    int initTimeout = 10000;
    int desiredReplicationFactor = 1;

    public KafkaStore(String kafkaTopic, Producer<String,T> kafkaProducer){
        this.kafkaTopic = kafkaTopic;
        this.kafkaProducer = kafkaProducer;
        this.kafkaStore = new ConcurrentHashMap<String,T>();
    }

    T produce(String key, T message){
        try {
            long startTime = System.currentTimeMillis();

            RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<>(kafkaTopic, key, message)).get(initTimeout, TimeUnit.MILLISECONDS);
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
        String operation=null;
        if(record.key().equals("NOOP")){
            //LOG.debug(kafkaTopic+" : NOOP");
        }else if (record.value()==null){
            operation="DELETE";
            kafkaStore.remove(record.key());
        }else {
            if(kafkaStore.containsKey(record.key()))
                operation="UPDATE";
            else
                operation="NEW";
            kafkaStore.put(record.key(), record.value());
            LOG.debug(String.format("%s %s %s",operation, kafkaTopic,record.key()));
        }

        lastReadOffset = record.offset();
    }

    @PostConstruct
    private void createOrVerifyTopic() throws StoreInitializationException {
        createOrVerifySchemaTopic(kafkaTopic);
        produceNOOP();
    }
    void produceNOOP() throws StoreInitializationException {
        try {
            LOG.trace("Sending Noop record to KafkaStore to find last offset.");
            Future<RecordMetadata> ack = kafkaProducer.send(new ProducerRecord<>(kafkaTopic,"NOOP",null));
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
            LOG.info("Target offset reached for kafkaTopic "+ kafkaTopic);
        }else{
            LOG.error("Server stop requested because could not read fully kafkaTopic "+ kafkaTopic);
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
                    "Timed out trying to create or validate kafkaTopic configuration",
                    e
            );
        } catch (InterruptedException | ExecutionException e) {
            throw new StoreInitializationException(
                    "Failed trying to create or validate kafkaTopic configuration",
                    e
            );
        }
    }

    private void createSchemaTopic(String topic) throws StoreInitializationException,
            InterruptedException,
            ExecutionException,
            TimeoutException {
        LOG.info("Creating kafkaTopic {}", topic);

        int numLiveBrokers = adminClient.describeCluster().nodes()
                .get(initTimeout, TimeUnit.MILLISECONDS).size();
        if (numLiveBrokers <= 0) {
            throw new StoreInitializationException("No live Kafka brokers");
        }

        int schemaTopicReplicationFactor = Math.min(numLiveBrokers, desiredReplicationFactor);
        if (schemaTopicReplicationFactor < desiredReplicationFactor) {
            LOG.warn("Creating the kafkaTopic "
                    + topic
                    + " using a replication factor of "
                    + schemaTopicReplicationFactor
                    + ", which is less than the desired one of "
                    + desiredReplicationFactor + ". If this is a production environment, it's "
                    + "crucial to add more brokers and increase the replication factor of the kafkaTopic.");
        }

        NewTopic schemaTopicRequest = new NewTopic(topic, 1, (short) schemaTopicReplicationFactor);
        schemaTopicRequest.configs(kafkaStoreConfig.getProperties());
        try {
            adminClient.createTopics(Collections.singleton(schemaTopicRequest)).all()
                    .get(initTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                // If kafkaTopic already exists, ensure that it is configured correctly.
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
        LOG.info("Validating kafkaTopic {}", topic);

        Set<String> topics = Collections.singleton(topic);
        Map<String, TopicDescription> topicDescription = adminClient.describeTopics(topics)
                .all().get(initTimeout, TimeUnit.MILLISECONDS);

        TopicDescription description = topicDescription.get(topic);
        final int numPartitions = description.partitions().size();
        if (numPartitions != 1) {
            throw new StoreInitializationException("The kafkaTopic " + topic + " should have only 1 "
                    + "partition but has " + numPartitions);
        }

        if (description.partitions().get(0).replicas().size() < desiredReplicationFactor) {
            LOG.warn("The replication factor of the kafkaTopic "
                    + topic
                    + " is less than the desired one of "
                    + desiredReplicationFactor
                    + ". If this is a production environment, it's crucial to add more brokers and "
                    + "increase the replication factor of the kafkaTopic.");
        }

        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        Map<ConfigResource, Config> configs =
                adminClient.describeConfigs(Collections.singleton(topicResource)).all()
                        .get(initTimeout, TimeUnit.MILLISECONDS);
        Config topicConfigs = configs.get(topicResource);
        String retentionPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
        if (retentionPolicy == null || !TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
            LOG.error("The retention policy of the kafkaTopic " + topic + " is incorrect. "
                    + "You must configure the kafkaTopic to 'compact' cleanup policy to avoid Kafka "
                    + "deleting your data after a week. "
                    + "Refer to Kafka documentation for more details on cleanup policies");

            throw new StoreInitializationException("The retention policy of the schema kafkaTopic " + topic
                    + " is incorrect. Expected cleanup.policy to be "
                    + "'compact' but it is " + retentionPolicy);

        }
    }
    // END http://www.confluent.io/confluent-community-license

}
