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

import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.TopicRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.TaskScheduler;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/** Kafka Topic repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaTopicRepository extends KafkaStore<Topic> implements TopicRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     * @param adminClient The Kafka admin client
     * @param ns4KafkaProperties Ns4Kafka properties
     * @param taskScheduler The task scheduler
     */
    public KafkaTopicRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.topics") String kafkaTopic,
            @KafkaClient("topics-producer") Producer<String, Topic> kafkaProducer,
            AdminClient adminClient,
            Ns4KafkaProperties ns4KafkaProperties,
            @Named(TaskExecutors.SCHEDULED) TaskScheduler taskScheduler) {
        super(kafkaTopic, kafkaProducer, adminClient, ns4KafkaProperties, taskScheduler);
    }

    /**
     * Get the message key for a given topic.
     *
     * @param topic The message
     * @return The message key
     */
    @Override
    public String getMessageKey(Topic topic) {
        return topic.getMetadata().getCluster() + "/" + topic.getMetadata().getName();
    }

    /**
     * Find all topics.
     *
     * @return The list of topics
     */
    @Override
    public List<Topic> findAll() {
        return new ArrayList<>(getKafkaStore().values());
    }

    /**
     * Find all topics by cluster.
     *
     * @param cluster The cluster
     * @return The list of topics
     */
    @Override
    public List<Topic> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(topic -> topic.getMetadata().getCluster().equals(cluster))
                .toList();
    }

    /**
     * Find a topic by name and cluster.
     *
     * @param cluster The cluster
     * @param name The topic name
     * @return An optional topic
     */
    @Override
    public Optional<Topic> findByName(String cluster, String name) {
        return Optional.ofNullable(getKafkaStore().get(cluster + "/" + name));
    }

    /**
     * Create a given topic.
     *
     * @param topic The topic to create
     * @return The created topic
     */
    @Override
    public Topic create(Topic topic) {
        return this.produce(getMessageKey(topic), topic);
    }

    /**
     * Delete a given topic.
     *
     * @param topic The topic to delete
     */
    @Override
    public void delete(Topic topic) {
        this.produce(getMessageKey(topic), null);
    }

    /** @param message The record */
    @Override
    @io.micronaut.configuration.kafka.annotation.Topic(value = "${ns4kafka.store.kafka.topics.prefix}.topics")
    public void receive(ConsumerRecord<String, Topic> message) {
        super.receive(message);
    }
}
