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

import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.repository.SchemaRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/** Kafka Schemas repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaSchemaRepository extends KafkaStore<Schema> implements SchemaRepository {
    /**
     * Constructor.
     *
     * @param kafkaTopic The schemas topic
     * @param kafkaProducer The schemas kafka producer
     */
    public KafkaSchemaRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.schemas") String kafkaTopic,
            @KafkaClient("schemas-producer") Producer<String, Schema> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(Schema schema) {
        return schema.getMetadata().getCluster() + "/" + schema.getMetadata().getName();
    }

    /**
     * Create a given schema.
     *
     * @param schema The schema to create
     * @return The created schema
     */
    @Override
    public Schema create(Schema schema) {
        return this.produce(getMessageKey(schema), schema);
    }

    /**
     * Delete a given schema.
     *
     * @param schema The schema to delete
     */
    @Override
    public void delete(Schema schema) {
        this.produce(getMessageKey(schema), null);
    }

    @Override
    @io.micronaut.configuration.kafka.annotation.Topic(value = "${ns4kafka.store.kafka.topics.prefix}.schemas")
    void receive(ConsumerRecord<String, Schema> message) {
        super.receive(message);
    }

    /**
     * Find all schemas.
     *
     * @return The list of schemas
     */
    @Override
    public List<Schema> findAll() {
        return new ArrayList<>(getKafkaStore().values());
    }

    /**
     * Find all schemas by cluster.
     *
     * @param cluster The cluster
     * @return The list of schemas
     */
    @Override
    public List<Schema> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(schema -> schema.getMetadata().getCluster().equals(cluster))
                .toList();
    }
}
