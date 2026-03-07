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

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.repository.NamespaceRepository;
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

/** Kafka Namespace repository. */
@Singleton
public class KafkaNamespaceRepository extends KafkaStore<Namespace> implements NamespaceRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     */
    public KafkaNamespaceRepository(
            KafkaStreams kafkaStreams,
            @Value("${ns4kafka.store.kafka.topics.prefix}." + InternalTopic.NAMESPACE) String kafkaTopic,
            @KafkaClient("namespace-producer") Producer<String, Namespace> kafkaProducer) {
        super(kafkaStreams, kafkaTopic, kafkaProducer);
    }

    @Override
    public String getMessageKey(Namespace namespace) {
        return namespace.getMetadata().getName();
    }

    @Override
    public List<Namespace> findAllForCluster(String cluster) {
        List<Namespace> results = new ArrayList<>();
        try (KeyValueIterator<String, Namespace> iterator = queryAllStore()) {
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

    @Override
    public Optional<Namespace> findByName(String namespace) {
        return Optional.ofNullable(queryStoreByKey(namespace));
    }

    /**
     * Create a namespace.
     *
     * @param namespace The namespace to create
     */
    @Override
    public void create(Namespace namespace) {
        produce(getMessageKey(namespace), namespace);
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
}
