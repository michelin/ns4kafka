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

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.TaskScheduler;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/** Kafka Namespace repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaNamespaceRepository extends KafkaStore<Namespace> implements NamespaceRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     * @param adminClient The Kafka admin client
     * @param ns4KafkaProperties Ns4Kafka properties
     * @param taskScheduler The task scheduler
     */
    public KafkaNamespaceRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.namespaces") String kafkaTopic,
            @KafkaClient("namespace-producer") Producer<String, Namespace> kafkaProducer,
            AdminClient adminClient,
            Ns4KafkaProperties ns4KafkaProperties,
            @Named(TaskExecutors.SCHEDULED) TaskScheduler taskScheduler) {
        super(kafkaTopic, kafkaProducer, adminClient, ns4KafkaProperties, taskScheduler);
    }

    /**
     * Get the message key for a namespace.
     *
     * @param namespace The message
     * @return The message key
     */
    @Override
    public String getMessageKey(Namespace namespace) {
        return namespace.getMetadata().getName();
    }

    /**
     * Find all namespaces for a cluster.
     *
     * @param cluster The cluster name
     * @return The list of namespaces for the cluster
     */
    @Override
    public List<Namespace> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(namespace -> namespace.getMetadata().getCluster().equals(cluster))
                .toList();
    }

    /**
     * Find a namespace by name.
     *
     * @param namespace The namespace name
     * @return The namespace if found, empty otherwise
     */
    @Override
    public Optional<Namespace> findByName(String namespace) {
        return Optional.ofNullable(getKafkaStore().get(namespace));
    }

    /**
     * Create a namespace.
     *
     * @param namespace The namespace to create
     * @return The created namespace
     */
    @Override
    public Namespace create(Namespace namespace) {
        return produce(getMessageKey(namespace), namespace);
    }

    /**
     * Delete a namespace.
     *
     * @param namespace The namespace to delete
     */
    @Override
    public void delete(Namespace namespace) {
        produce(getMessageKey(namespace), null);
    }

    /**
     * Receive a namespace record from Kafka and update the store.
     *
     * @param message The record
     */
    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.namespaces")
    void receive(ConsumerRecord<String, Namespace> message) {
        super.receive(message);
    }
}
