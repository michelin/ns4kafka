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

import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import com.michelin.ns4kafka.repository.kafka.InternalTopic;
import com.michelin.ns4kafka.repository.kafka.KafkaStore;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;

/** Kafka Connector repository. */
@Singleton
public class KafkaConnectorRepository extends KafkaStore<Connector> implements ConnectorRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     */
    public KafkaConnectorRepository(
            KafkaStreams kafkaStreams,
            @Value("${ns4kafka.store.kafka.topics.prefix}." + InternalTopic.CONNECTOR) String kafkaTopic,
            @KafkaClient("connectors-producer") Producer<String, Connector> kafkaProducer) {
        super(kafkaStreams, kafkaTopic, kafkaProducer);
    }

    @Override
    public String getMessageKey(Connector connector) {
        return connector.getMetadata().getNamespace() + "/"
                + connector.getMetadata().getName();
    }

    /**
     * Find all connectors by cluster.
     *
     * @param cluster The cluster
     * @return The list of connectors
     */
    @Override
    public List<Connector> findAllForCluster(String cluster) {
        List<Connector> results = new ArrayList<>();
        try (KeyValueIterator<String, Connector> iterator = queryAllStore()) {
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
     * Create a connector.
     *
     * @param connector The connector to create
     */
    @Override
    public void create(Connector connector) {
        produce(getMessageKey(connector), connector);
    }

    /**
     * Delete a connector.
     *
     * @param connector The connector to delete
     */
    @Override
    public void delete(Connector connector) {
        produce(getMessageKey(connector), null);
    }
}
