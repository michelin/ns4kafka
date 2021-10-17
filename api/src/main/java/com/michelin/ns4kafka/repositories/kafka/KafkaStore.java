package com.michelin.ns4kafka.repositories.kafka;

import io.micronaut.configuration.kafka.config.KafkaConsumerConfiguration;
import io.micronaut.configuration.kafka.config.KafkaProducerConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.apache.kafka.connect.util.TopicAdmin;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public abstract class KafkaStore<T> {

    @Inject @Named("default") KafkaProducerConfiguration<String, T> producerConfigs;
    @Inject @Named("default") KafkaConsumerConfiguration<String, T> consumerConfigs;
    @Inject SharedTopicAdmin sharedTopicAdmin;
    @Inject KafkaStoreConfig kafkaStoreConfig;
    private KafkaBasedLog<String, T> configLog;
    private final Map<String,T> kafkaStore = new HashMap<>();
    private final String topic;
    private volatile boolean started;
    private volatile long offset;


    private final Object lock;

    private static final long READ_TO_END_TIMEOUT_MS = 30000;

    public KafkaStore(String topic){
        this.lock = new Object();
        this.started = false;
        this.topic = topic;
        this.offset = -1;
    }
    private void configure(){

        Map<String, Object> producerConfigsMap = new HashMap<>();
        producerConfigs.getConfig().forEach((key, value) -> producerConfigsMap.put(key.toString(), value.toString()));

        Map<String, Object> consumerConfigsMap = new HashMap<>();
        consumerConfigs.getConfig().forEach((key, value) -> consumerConfigsMap.put(key.toString(), value.toString()));

        NewTopic topicDescription = TopicAdmin.defineTopic(topic)
                .config((Map) kafkaStoreConfig.getProperties()) // first so that we override user-supplied settings as needed
                .compacted()
                .partitions(1)
                .replicationFactor((short)kafkaStoreConfig.getReplicationFactor())
                .build();

        java.util.function.Consumer<TopicAdmin> createTopics = admin -> {
            log.debug("Creating admin client to manage Connect internal config topic");
            // Create the topic if it doesn't exist
            Set<String> newTopics = admin.createTopics(topicDescription);
            if (!newTopics.contains(topic)) {
                // It already existed, so check that the topic cleanup policy is compact only and not delete
                log.debug("Using admin client to check cleanup policy of '{}' topic is '{}'", topic, TopicConfig.CLEANUP_POLICY_COMPACT);
                admin.verifyTopicCleanupPolicyOnlyCompact(topic, "ns4kafka.store.topics.props", "store");
            }
        };

        this.configLog = new KafkaBasedLog<>(topic, producerConfigsMap, consumerConfigsMap, sharedTopicAdmin, new ConsumeCallback(), Time.SYSTEM, createTopics);
    }

    public void start() {
        configure();
        log.info("Starting KafkaConfigBackingStore");
        // Before startup, callbacks are *not* invoked. You can grab a snapshot after starting -- just take care that
        // updates can continue to occur in the background
        configLog.start();

        int partitionCount = configLog.partitionCount();
        if (partitionCount > 1) {
            String msg = String.format("Topic '%s' is required "
                            + "to have a single partition in order to guarantee consistency of "
                            + "store configurations, but found %d partitions.",
                    topic, partitionCount);
            throw new KafkaStoreException(msg);
        }

        started = true;
        log.info("Started KafkaConfigBackingStore");
    }

    public Map<String,T> getKafkaStore(){
        return kafkaStore;
    }

    abstract String getMessageKey(T message);

    T produce(String key, T message) throws KafkaStoreException {
        if (key == null) {
            throw new KafkaStoreException("Key must not be null");
        }

        try {
            configLog.send(key, message);
            configLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to write configuration to Kafka: ", e);
            throw new KafkaStoreException("Error writing configuration to Kafka", e);
        }
        return kafkaStore.get(key);
    }

    @SuppressWarnings("unchecked")
    private class ConsumeCallback implements Callback<ConsumerRecord<String, T>> {
        @Override
        public void onCompletion(Throwable error, ConsumerRecord<String, T> record) {
            if (error != null) {
                log.error("Unexpected in consumer callback for KafkaConfigBackingStore: ", error);
                return;
            }
            offset = record.offset() + 1;

            if (!record.key().equals("NOOP")) {
                synchronized (lock) {
                    T message = record.value();

                    log.trace("Applying update ({},{}) to the local store", record.key(), message);

                    if (message == null) {
                        kafkaStore.remove(record.key());
                    } else {
                        kafkaStore.put(record.key(), message);
                    }
                }
            }
        }
    }
}
