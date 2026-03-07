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

import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.repository.ConnectClusterRepository;
import com.michelin.ns4kafka.repository.kafka.InternalTopic;
import com.michelin.ns4kafka.repository.kafka.KafkaStore;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;

/** Kafka Connect Cluster repository. */
@Singleton
public class KafkaConnectClusterRepository extends KafkaStore<ConnectCluster> implements ConnectClusterRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     */
    public KafkaConnectClusterRepository(
            KafkaStreams kafkaStreams,
            @Value("${ns4kafka.store.kafka.topics.prefix}." + InternalTopic.CONNECT_CLUSTER) String kafkaTopic,
            @KafkaClient("connect-workers") Producer<String, ConnectCluster> kafkaProducer) {
        super(kafkaStreams, kafkaTopic, kafkaProducer);
    }

    /**
     * Get the message key for a given Kafka Connect cluster.
     *
     * @param connectCluster The message
     * @return The message key
     */
    @Override
    public String getMessageKey(ConnectCluster connectCluster) {
        return connectCluster.getMetadata().getNamespace() + "/"
                + connectCluster.getMetadata().getName();
    }

    /**
     * Find all Kafka Connect clusters.
     *
     * @return The list of Kafka Connect clusters
     */
    @Override
    public Collection<ConnectCluster> findAll() {
        List<ConnectCluster> results = new ArrayList<>();
        try (KeyValueIterator<String, ConnectCluster> iterator = queryAllStore()) {
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
     * Find all Kafka Connect clusters for a given cluster.
     *
     * @param cluster The cluster name
     * @return The list of Kafka Connect clusters for the given cluster
     */
    @Override
    public List<ConnectCluster> findAllForCluster(String cluster) {
        List<ConnectCluster> results = new ArrayList<>();
        try (KeyValueIterator<String, ConnectCluster> iterator = queryAllStore()) {
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
     * Create a Kafka Connect cluster.
     *
     * @param connectCluster The Kafka Connect cluster to create
     */
    @Override
    public void create(ConnectCluster connectCluster) {
        produce(getMessageKey(connectCluster), connectCluster);
    }

    /**
     * Delete a Kafka Connect cluster.
     *
     * @param connectCluster The Kafka Connect cluster to delete
     */
    @Override
    public void delete(ConnectCluster connectCluster) {
        produce(getMessageKey(connectCluster), null);
    }
}
