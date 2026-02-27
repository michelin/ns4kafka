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

import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.ConnectClusterRepository;
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
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/** Kafka Connect Cluster repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaConnectClusterRepository extends KafkaStore<ConnectCluster> implements ConnectClusterRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     * @param adminClient The Kafka admin client
     * @param ns4KafkaProperties Ns4Kafka properties
     * @param taskScheduler The task scheduler
     */
    public KafkaConnectClusterRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.connect-workers") String kafkaTopic,
            @KafkaClient("connect-workers") Producer<String, ConnectCluster> kafkaProducer,
            AdminClient adminClient,
            Ns4KafkaProperties ns4KafkaProperties,
            @Named(TaskExecutors.SCHEDULED) TaskScheduler taskScheduler) {
        super(kafkaTopic, kafkaProducer, adminClient, ns4KafkaProperties, taskScheduler);
    }

    /**
     * Find all connect clusters.
     *
     * @return The list of connect clusters
     */
    @Override
    public Collection<ConnectCluster> findAll() {
        return getKafkaStore().values();
    }

    /**
     * Find all connect clusters for a given cluster.
     *
     * @param cluster The cluster
     * @return The list of connect clusters
     */
    @Override
    public List<ConnectCluster> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(connectCluster ->
                        connectCluster.getMetadata().getCluster().equals(cluster))
                .toList();
    }

    /**
     * Create or update a connect cluster.
     *
     * @param connectCluster The connect cluster
     * @return The created or updated connect cluster
     */
    @Override
    public ConnectCluster create(ConnectCluster connectCluster) {
        return this.produce(getMessageKey(connectCluster), connectCluster);
    }

    /**
     * Delete a connect cluster.
     *
     * @param connectCluster The connect cluster
     */
    @Override
    public void delete(ConnectCluster connectCluster) {
        this.produce(getMessageKey(connectCluster), null);
    }

    /**
     * Receive a connect cluster record.
     *
     * @param message The record
     */
    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.connect-workers")
    void receive(ConsumerRecord<String, ConnectCluster> message) {
        super.receive(message);
    }

    /**
     * Get the message key for a given connect cluster.
     *
     * @param connectCluster The message
     * @return The message key
     */
    @Override
    String getMessageKey(ConnectCluster connectCluster) {
        return connectCluster.getMetadata().getNamespace() + "/"
                + connectCluster.getMetadata().getName();
    }
}
