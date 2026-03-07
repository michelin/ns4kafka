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
package com.michelin.ns4kafka.repository.kafka.repository;

import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.repository.TopicRepository;
import com.michelin.ns4kafka.repository.kafka.InternalTopic;
import com.michelin.ns4kafka.repository.kafka.KafkaStore;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;

/** Kafka Topic repository. */
@Singleton
public class KafkaTopicRepository extends KafkaStore<Topic> implements TopicRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     */
    public KafkaTopicRepository(
            KafkaStreams kafkaStreams,
            @Value("${ns4kafka.store.kafka.topics.prefix}." + InternalTopic.TOPIC) String kafkaTopic,
            @KafkaClient("topics-producer") Producer<String, Topic> kafkaProducer) {
        super(kafkaStreams, kafkaTopic, kafkaProducer);
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
        List<Topic> results = new ArrayList<>();
        try (KeyValueIterator<String, Topic> iterator = queryAllStore()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (entry.value != null) {
                    results.add(entry.value);
                }
            }
        }
        return results;
    }

    /**
     * Find all topics by cluster.
     *
     * @param cluster The cluster
     * @return The list of topics
     */
    @Override
    public List<Topic> findAllForCluster(String cluster) {
        List<Topic> results = new ArrayList<>();
        try (KeyValueIterator<String, Topic> iterator = queryAllStore()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (entry.value != null
                        && entry.value.getMetadata().getCluster().equals(cluster)) {
                    results.add(entry.value);
                }
            }
        }
        return results;
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
        return Optional.ofNullable(queryStoreByKey(cluster + "/" + name));
    }

    /**
     * Create a given topic.
     *
     * @param topic The topic to create
     */
    @Override
    public void create(Topic topic) {
        produce(getMessageKey(topic), topic);
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
}
