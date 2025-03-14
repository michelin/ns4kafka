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

import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/** Kafka Connector repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaConnectorRepository extends KafkaStore<Connector> implements ConnectorRepository {
    public KafkaConnectorRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.connectors") String kafkaTopic,
            @KafkaClient("connectors-producer") Producer<String, Connector> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(Connector connector) {
        return connector.getMetadata().getNamespace() + "/"
                + connector.getMetadata().getName();
    }

    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.connectors")
    void receive(ConsumerRecord<String, Connector> message) {
        super.receive(message);
    }

    /**
     * Create a given connector.
     *
     * @param connector The connector to create
     * @return The created connector
     */
    @Override
    public Connector create(Connector connector) {
        return this.produce(getMessageKey(connector), connector);
    }

    /**
     * Delete a given connector.
     *
     * @param connector The connector to delete
     */
    @Override
    public void delete(Connector connector) {
        this.produce(getMessageKey(connector), null);
    }

    /**
     * Find all connectors by cluster.
     *
     * @param cluster The cluster
     * @return The list of connectors
     */
    @Override
    public List<Connector> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(connector -> connector.getMetadata().getCluster().equals(cluster))
                .toList();
    }
}
