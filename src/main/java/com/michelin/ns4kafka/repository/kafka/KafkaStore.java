/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.ns4kafka.repository.kafka;

import com.michelin.ns4kafka.property.KafkaStoreProperties;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.TaskScheduler;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * Kafka store.
 *
 * @param <T> The type of the store
 */
@Slf4j
public abstract class KafkaStore<T> {
    @Inject
    ApplicationContext applicationContext;

    @Inject
    AdminClient adminClient;

    @Inject
    KafkaStoreProperties kafkaStoreProperties;

    @Inject
    @Named(TaskExecutors.SCHEDULED)
    TaskScheduler taskScheduler;

    @Property(name = "ns4kafka.store.kafka.init-timeout")
    int initTimeout;

    private final Map<String, T> store;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ReentrantLock offsetUpdateLock;
    private final Condition offsetReachedThreshold;
    String kafkaTopic;
    Producer<String, T> kafkaProducer;
    long offsetInSchemasTopic = -1;
    long lastWrittenOffset = -1;

    KafkaStore(String kafkaTopic, Producer<String, T> kafkaProducer) {
        this.kafkaTopic = kafkaTopic;
        this.kafkaProducer = kafkaProducer;
        this.store = new ConcurrentHashMap<>();
        this.offsetUpdateLock = new ReentrantLock();
        this.offsetReachedThreshold = offsetUpdateLock.newCondition();
    }

    /**
     * Get message key.
     *
     * @param message The message
     * @return The key of the message
     */
    abstract String getMessageKey(T message);

    /**
     * Create or verify the internal topic.
     *
     * @throws KafkaStoreException Exception thrown during internal topic creation or verification
     */
    @PostConstruct
    private void createOrVerifyTopic() throws KafkaStoreException {
        createOrVerifyInternalTopic();
        taskScheduler.schedule(Duration.ZERO, this::waitUntilKafkaReaderReachesLastOffsetInit);
    }

    /**
     * Create or verify the internal kafka topic.
     *
     * @throws KafkaStoreException Exception thrown during internal topic creation or verification
     */
    private void createOrVerifyInternalTopic() throws KafkaStoreException {
        try {
            Set<String> allTopics = adminClient.listTopics()
                .names()
                .get(initTimeout, TimeUnit.MILLISECONDS);

            if (allTopics.contains(kafkaTopic)) {
                verifyInternalTopic();
            } else {
                createInternalTopic();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaStoreException(
                "Thread interrupted trying to create or validate configuration of topic " + kafkaTopic + ".", e);
        } catch (ExecutionException e) {
            throw new KafkaStoreException(
                "Execution error trying to create or validate configuration of topic " + kafkaTopic + ".", e);
        } catch (TimeoutException e) {
            throw new KafkaStoreException(
                "Timed out trying to create or validate configuration of topic " + kafkaTopic + ".", e);
        }
    }

    /**
     * Verify the internal topic.
     *
     * @throws KafkaStoreException  Exception thrown during internal topic verification
     * @throws InterruptedException Exception thrown during internal topic verification
     * @throws ExecutionException   Exception thrown during internal topic verification
     * @throws TimeoutException     Exception thrown during internal topic verification
     */
    private void verifyInternalTopic()
        throws KafkaStoreException, InterruptedException, ExecutionException, TimeoutException {
        log.info("Validating topic {}.", kafkaTopic);

        Set<String> topics = Collections.singleton(kafkaTopic);
        Map<String, TopicDescription> topicDescription = adminClient.describeTopics(topics)
            .allTopicNames()
            .get(initTimeout, TimeUnit.MILLISECONDS);

        TopicDescription description = topicDescription.get(kafkaTopic);
        final int numPartitions = description.partitions().size();
        if (numPartitions != 1) {
            throw new KafkaStoreException(
                "The topic " + kafkaTopic + " should have only 1 partition but has " + numPartitions + ".");
        }

        if (description.partitions().getFirst().replicas().size() < kafkaStoreProperties.getReplicationFactor()
            && log.isWarnEnabled()) {
            log.warn("The replication factor of the topic " + kafkaTopic + " is less than the desired one of "
                + kafkaStoreProperties.getReplicationFactor()
                + ". If this is a production environment, it's crucial to add more brokers and "
                + "increase the replication factor of the topic.");
        }

        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, kafkaTopic);
        Map<ConfigResource, Config> configs = adminClient.describeConfigs(Collections.singleton(topicResource))
            .all()
            .get(initTimeout, TimeUnit.MILLISECONDS);

        Config topicConfigs = configs.get(topicResource);
        String retentionPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
        if (!TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
            if (log.isErrorEnabled()) {
                log.error("The retention policy of the topic " + kafkaTopic + " is incorrect. "
                    + "You must configure the topic to 'compact' cleanup policy to avoid Kafka "
                    + "deleting your data after a week. Refer to Kafka documentation for more details "
                    + "on cleanup policies");
            }

            throw new KafkaStoreException("The retention policy of the schema kafkaTopic " + kafkaTopic
                + " is incorrect. Expected cleanup.policy to be 'compact' but it is " + retentionPolicy);

        }
    }

    /**
     * Create the internal topic.
     *
     * @throws KafkaStoreException  Exception thrown during internal topic creation
     * @throws InterruptedException Exception thrown during internal topic creation
     * @throws ExecutionException   Exception thrown during internal topic creation
     * @throws TimeoutException     Exception thrown during internal topic creation
     */
    private void createInternalTopic()
        throws KafkaStoreException, InterruptedException, ExecutionException, TimeoutException {
        log.info("Creating topic {}.", kafkaTopic);

        int numLiveBrokers = adminClient.describeCluster()
            .nodes()
            .get(initTimeout, TimeUnit.MILLISECONDS)
            .size();

        if (numLiveBrokers == 0) {
            throw new KafkaStoreException("No live Kafka brokers.");
        }

        int schemaTopicReplicationFactor = Math.min(numLiveBrokers, kafkaStoreProperties.getReplicationFactor());
        if (schemaTopicReplicationFactor < kafkaStoreProperties.getReplicationFactor() && log.isWarnEnabled()) {
            log.warn("Creating the kafkaTopic " + kafkaTopic + " using a replication factor of "
                + schemaTopicReplicationFactor + ", which is less than the desired one of "
                + kafkaStoreProperties.getReplicationFactor() + ". If this is a production environment, it's "
                + "crucial to add more brokers and increase the replication factor of the kafkaTopic.");
        }

        NewTopic schemaTopicRequest = new NewTopic(kafkaTopic, 1, (short) schemaTopicReplicationFactor);
        schemaTopicRequest.configs(kafkaStoreProperties.getProps());

        try {
            adminClient.createTopics(Collections.singleton(schemaTopicRequest))
                .all()
                .get(initTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                verifyInternalTopic();
            } else {
                throw e;
            }
        }
    }

    /**
     * Get the current Kafka store.
     *
     * @return The Kafka store
     */
    public Map<String, T> getKafkaStore() {
        return store;
    }

    /**
     * Produce a new record.
     *
     * @param key     The record key
     * @param message The record body
     * @return The produced record
     * @throws KafkaStoreException Exception thrown during the send process
     */
    T produce(String key, T message) throws KafkaStoreException {
        if (key == null) {
            throw new KafkaStoreException("Key should not be null");
        }

        boolean knownSuccessfulWrite = false;
        try {
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(kafkaTopic, key, message);
            log.trace("Sending record to topic {}", producerRecord);
            Future<RecordMetadata> ack = kafkaProducer.send(producerRecord);
            RecordMetadata recordMetadata = ack.get(initTimeout, TimeUnit.MILLISECONDS);

            log.trace("Waiting for the local store to catch up to offset {}", recordMetadata.offset());
            lastWrittenOffset = recordMetadata.offset();
            waitUntilOffset(getLatestOffset(), TimeUnit.MILLISECONDS);
            knownSuccessfulWrite = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaStoreException("Put operation interrupted while waiting for an ack from Kafka", e);
        } catch (ExecutionException e) {
            throw new KafkaStoreException("Put operation failed while waiting for an ack from Kafka", e);
        } catch (TimeoutException e) {
            throw new KafkaStoreException("Put operation timed out while waiting for an ack from Kafka", e);
        } catch (KafkaException e) {
            throw new KafkaStoreException("Put operation to Kafka failed", e);
        } finally {
            if (!knownSuccessfulWrite) {
                this.lastWrittenOffset = -1;
            }
        }
        return store.get(key);
    }

    /**
     * Handle a new consumed record
     * See: /core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaStoreReaderThread.java#L326
     *
     * @param message The record
     */
    void receive(ConsumerRecord<String, T> message) {
        try {
            if (!message.key().equals("NOOP")) {
                log.trace("Applying update ({},{}) to the local store", message.key(), message.value());
                if (message.value() == null) {
                    store.remove(message.key());
                } else {
                    store.put(message.key(), message.value());
                }
            }

            try {
                offsetUpdateLock.lock();
                offsetInSchemasTopic = message.offset();
                offsetReachedThreshold.signalAll();
            } finally {
                offsetUpdateLock.unlock();
            }
        } catch (RuntimeException e) {
            log.error("KafkaStoreReader thread has died for an unknown reason.", e);
            throw new KafkaStoreException(e.getMessage());
        }
    }

    /**
     * Wait until the Kafka reader reaches the last offset.
     * Mark the store as initialized when it is done.
     */
    public void waitUntilKafkaReaderReachesLastOffsetInit() {
        try {
            waitUntilOffset(getLatestOffset(), TimeUnit.MILLISECONDS);
            boolean isInitialized = initialized.compareAndSet(false, true);
            if (!isInitialized) {
                throw new KafkaStoreException("Illegal state while initializing store. Store was already initialized");
            }
        } catch (Exception e) {
            log.error("Unrecoverable error during initialization", e);
        }
    }

    /**
     * Get latest offset.
     *
     * @return The latest offset
     * @throws KafkaStoreException Exception while getting the latest offset
     */
    private long getLatestOffset() throws KafkaStoreException {
        if (lastWrittenOffset >= 0) {
            return lastWrittenOffset;
        }

        try {
            log.trace("Sending NOOP record to topic {} to find last offset.", kafkaTopic);
            Future<RecordMetadata> ack = kafkaProducer.send(new ProducerRecord<>(kafkaTopic, "NOOP", null));
            RecordMetadata metadata = ack.get(initTimeout, TimeUnit.MILLISECONDS);
            this.lastWrittenOffset = metadata.offset();
            log.trace("NOOP record's offset is {}", lastWrittenOffset);
            return lastWrittenOffset;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaStoreException(
                "Thread interrupted while waiting for the latest offset of topic " + kafkaTopic + ".", e);
        } catch (ExecutionException e) {
            throw new KafkaStoreException(
                "Execution error while waiting for the latest offset of topic " + kafkaTopic + ".", e);
        } catch (TimeoutException e) {
            throw new KafkaStoreException("Timeout while waiting for the latest offset of topic " + kafkaTopic + ".",
                e);
        } catch (Exception e) {
            throw new KafkaStoreException("Error while waiting for the latest offset of topic " + kafkaTopic + ".", e);
        }
    }

    /**
     * Wait until the given offset is read.
     *
     * @param offset   The offset
     * @param timeUnit The time unit to wait
     * @throws KafkaStoreException Exception thrown during the wait process
     */
    public void waitUntilOffset(long offset, TimeUnit timeUnit) throws KafkaStoreException {
        if (offset < 0) {
            throw new KafkaStoreException("Cannot wait for a negative offset.");
        }

        log.trace("Waiting to read offset {}. Currently at offset {}.", offset, offsetInSchemasTopic);

        try {
            offsetUpdateLock.lock();
            long timeoutNs = TimeUnit.NANOSECONDS.convert(initTimeout, timeUnit);
            while ((offsetInSchemasTopic < offset) && (timeoutNs > 0)) {
                try {
                    timeoutNs = offsetReachedThreshold.awaitNanos(timeoutNs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.debug("Interrupted while waiting for the background store reader thread "
                            + "to reach the specified offset: {}",
                        offset, e);
                }
            }
        } finally {
            offsetUpdateLock.unlock();
        }

        if (offsetInSchemasTopic < offset) {
            throw new KafkaStoreException("Failed to reach target offset within the timeout interval. targetOffset: "
                + offset + ", offsetReached: " + offsetInSchemasTopic + ", timeout(ms): "
                + TimeUnit.MILLISECONDS.convert(initTimeout, timeUnit));
        }
    }

    /**
     * Is the store initialized.
     *
     * @return true if it is, false otherwise
     */
    public boolean isInitialized() {
        return initialized.get();
    }

    /**
     * Report the init process.
     */
    public void reportInitProgress() {
        if (isInitialized()) {
            log.info("{} is ready! ({} records)", kafkaTopic, store.size());
        } else {
            log.info("Init in progress for {}... ({}/{})", kafkaTopic, offsetInSchemasTopic, lastWrittenOffset);
        }
    }
}
