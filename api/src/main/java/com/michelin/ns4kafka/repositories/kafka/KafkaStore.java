package com.michelin.ns4kafka.repositories.kafka;

import io.micronaut.context.ApplicationContext;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.TaskScheduler;
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

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public abstract class KafkaStore<T> {

    @Inject ApplicationContext applicationContext;
    @Inject AdminClient adminClient;

    @Inject KafkaStoreConfig kafkaStoreConfig;

    @Inject @Named(TaskExecutors.SCHEDULED) TaskScheduler taskScheduler;

    private Map<String,T> kafkaStore;
    String kafkaTopic;
    Producer<String,T> kafkaProducer;
    long offsetInSchemasTopic = -1;
    long lastWrittenOffset = -1;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ReentrantLock offsetUpdateLock;
    private final Condition offsetReachedThreshold;
    int initTimeout = 10000;

    public KafkaStore(String kafkaTopic, Producer<String,T> kafkaProducer){
        this.kafkaTopic = kafkaTopic;
        this.kafkaProducer = kafkaProducer;
        this.kafkaStore = new ConcurrentHashMap<String,T>();
        this.offsetUpdateLock = new ReentrantLock();
        this.offsetReachedThreshold = offsetUpdateLock.newCondition();

    }
    public Map<String,T> getKafkaStore(){
        return kafkaStore;
    }

    abstract String getMessageKey(T message);

    T produce(String key, T message) throws KafkaStoreException {
        if (key == null) {
            throw new KafkaStoreException("Key should not be null");
        }
        T oldValue = kafkaStore.get(key);


        boolean knownSuccessfulWrite = false;
        try {
            ProducerRecord<String,T> producerRecord = new ProducerRecord<>(kafkaTopic, key, message);
            log.trace("Sending record to KafkaStore topic: " + producerRecord);
            Future<RecordMetadata> ack = kafkaProducer.send(producerRecord);
            RecordMetadata recordMetadata = ack.get(initTimeout, TimeUnit.MILLISECONDS);

            log.trace("Waiting for the local store to catch up to offset " + recordMetadata.offset());
            this.lastWrittenOffset = recordMetadata.offset();
            waitUntilKafkaReaderReachesLastOffset(initTimeout);
            knownSuccessfulWrite = true;
        } catch (InterruptedException e) {
            throw new KafkaStoreException("Put operation interrupted while waiting for an ack from Kafka", e);
        } catch (ExecutionException e) {
            throw new KafkaStoreException("Put operation failed while waiting for an ack from Kafka", e);
        } catch (TimeoutException e) {
            throw new KafkaStoreException(
                    "Put operation timed out while waiting for an ack from Kafka", e);
        } catch (KafkaException ke) {
            throw new KafkaStoreException("Put operation to Kafka failed", ke);
        } finally {
            if (!knownSuccessfulWrite) {
                this.lastWrittenOffset = -1;
            }
        }
        return kafkaStore.get(key);
    }
    // mimics /core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaStoreReaderThread.java#L326 doWork
    void receive(ConsumerRecord<String, T> record) {
        try
        {
            String messageKey = record.key();
            if (messageKey.equals("NOOP")) {
                // If it's a noop, update local offset counter and do nothing else
                try {
                    offsetUpdateLock.lock();
                    offsetInSchemasTopic = record.offset();
                    offsetReachedThreshold.signalAll();
                } finally {
                    offsetUpdateLock.unlock();
                }
            } else {
                T message = record.value();

                log.trace("Applying update ("
                        + messageKey
                        + ","
                        + message
                        + ") to the local store");
                long offset = record.offset();

                T oldMessage;
                if (message == null) {
                    oldMessage = kafkaStore.remove(messageKey);
                } else {
                    oldMessage = kafkaStore.put(messageKey, message);
                }

                try {
                    offsetUpdateLock.lock();
                    offsetInSchemasTopic = record.offset();
                    offsetReachedThreshold.signalAll();
                } finally {
                    offsetUpdateLock.unlock();
                }

            }
        } catch (RuntimeException e) {
            log.error("KafkaStoreReader thread has died for an unknown reason.", e);
            throw new RuntimeException(e);
        }
    }

    @PostConstruct
    private void createOrVerifyTopic() throws KafkaStoreException {
        createOrVerifySchemaTopic(kafkaTopic);
        taskScheduler.schedule(Duration.ZERO, this::waitUntilKafkaReaderReachesLastOffsetInit);
    }
    public void waitUntilKafkaReaderReachesLastOffsetInit(){
        waitUntilKafkaReaderReachesLastOffset(initTimeout);
        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new KafkaStoreException("Illegal state while initializing store. Store "
                    + "was already initialized");
        }
    }

    // BEGIN http://www.confluent.io/confluent-community-license
    public void waitUntilKafkaReaderReachesLastOffset(int timeoutMs) throws KafkaStoreException {
        long offsetOfLastMessage = getLatestOffset(timeoutMs);
        waitUntilOffset(offsetOfLastMessage, timeoutMs, TimeUnit.MILLISECONDS);
    }
    private long getLatestOffset(int timeoutMs) throws KafkaStoreException {
        if (this.lastWrittenOffset >= 0) {
            return this.lastWrittenOffset;
        }

        try {
            log.trace("Sending Noop record to KafkaStore to find last offset.");
            Future<RecordMetadata> ack = kafkaProducer.send(new ProducerRecord<>(kafkaTopic,"NOOP",null));
            RecordMetadata metadata = ack.get(initTimeout, TimeUnit.MILLISECONDS);
            this.lastWrittenOffset = metadata.offset();
            log.trace("Noop record's offset is " + this.lastWrittenOffset);
            return this.lastWrittenOffset;
        } catch (Exception e) {
            throw new KafkaStoreException("Failed to write Noop record to kafka store.", e);
        }
    }

    private void createOrVerifySchemaTopic(String topic) throws KafkaStoreException {

        try {
            Set<String> allTopics = adminClient.listTopics().names().get(initTimeout, TimeUnit.MILLISECONDS);
            if (allTopics.contains(topic)) {
                verifySchemaTopic(topic);
            } else {
                createSchemaTopic(topic);
            }
        } catch (TimeoutException e) {
            throw new KafkaStoreException(
                    "Timed out trying to create or validate kafkaTopic configuration",
                    e
            );
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaStoreException(
                    "Failed trying to create or validate kafkaTopic configuration",
                    e
            );
        }
    }
    public void waitUntilOffset(long offset, long timeout, TimeUnit timeUnit) throws KafkaStoreException {
        if (offset < 0) {
            throw new KafkaStoreException("KafkaStoreReaderThread can't wait for a negative offset.");
        }

        log.trace("Waiting to read offset {}. Currently at offset {}", offset, offsetInSchemasTopic);

        try {
            offsetUpdateLock.lock();
            long timeoutNs = TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
            while ((offsetInSchemasTopic < offset) && (timeoutNs > 0)) {
                try {
                    timeoutNs = offsetReachedThreshold.awaitNanos(timeoutNs);
                } catch (InterruptedException e) {
                    log.debug("Interrupted while waiting for the background store reader thread to reach"
                            + " the specified offset: " + offset, e);
                }
            }
        } finally {
            offsetUpdateLock.unlock();
        }

        if (offsetInSchemasTopic < offset) {
            throw new KafkaStoreException(
                    "KafkaStoreReaderThread failed to reach target offset within the timeout interval. "
                            + "targetOffset: " + offset + ", offsetReached: " + offsetInSchemasTopic
                            + ", timeout(ms): " + TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
        }
    }

    private void createSchemaTopic(String topic) throws KafkaStoreException,
            InterruptedException,
            ExecutionException,
            TimeoutException {
        log.info("Creating kafkaTopic {}", topic);

        int numLiveBrokers = adminClient.describeCluster().nodes()
                .get(initTimeout, TimeUnit.MILLISECONDS).size();
        if (numLiveBrokers <= 0) {
            throw new KafkaStoreException("No live Kafka brokers");
        }

        int schemaTopicReplicationFactor = Math.min(numLiveBrokers, kafkaStoreConfig.getReplicationFactor());
        if (schemaTopicReplicationFactor < kafkaStoreConfig.getReplicationFactor()) {
            log.warn("Creating the kafkaTopic "
                    + topic
                    + " using a replication factor of "
                    + schemaTopicReplicationFactor
                    + ", which is less than the desired one of "
                    + kafkaStoreConfig.getReplicationFactor() + ". If this is a production environment, it's "
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

    private void verifySchemaTopic(String topic) throws KafkaStoreException,
            InterruptedException,
            ExecutionException,
            TimeoutException {
        log.info("Validating kafkaTopic {}", topic);

        Set<String> topics = Collections.singleton(topic);
        Map<String, TopicDescription> topicDescription = adminClient.describeTopics(topics)
                .all().get(initTimeout, TimeUnit.MILLISECONDS);

        TopicDescription description = topicDescription.get(topic);
        final int numPartitions = description.partitions().size();
        if (numPartitions != 1) {
            throw new KafkaStoreException("The kafkaTopic " + topic + " should have only 1 "
                    + "partition but has " + numPartitions);
        }

        if (description.partitions().get(0).replicas().size() < kafkaStoreConfig.getReplicationFactor()) {
            log.warn("The replication factor of the kafkaTopic "
                    + topic
                    + " is less than the desired one of "
                    + kafkaStoreConfig.getReplicationFactor()
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
            log.error("The retention policy of the kafkaTopic " + topic + " is incorrect. "
                    + "You must configure the kafkaTopic to 'compact' cleanup policy to avoid Kafka "
                    + "deleting your data after a week. "
                    + "Refer to Kafka documentation for more details on cleanup policies");

            throw new KafkaStoreException("The retention policy of the schema kafkaTopic " + topic
                    + " is incorrect. Expected cleanup.policy to be "
                    + "'compact' but it is " + retentionPolicy);

        }
    }

    public boolean isInitialized(){
        return initialized.get();
    }
    // END http://www.confluent.io/confluent-community-license

    protected String sha256_last8(String originalString) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(
                    originalString.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder(2 * encodedhash.length);
            for (int i = 0; i < encodedhash.length; i++) {
                String hex = Integer.toHexString(0xff & encodedhash[i]);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString().substring(56);

        } catch (NoSuchAlgorithmException e) {
            log.error("NoSuchAlgorithmException",e);
        }
        //should anything happen, we still need a unique K (might not be urlencode compliant but goodenough)
        return originalString;
    }

}
