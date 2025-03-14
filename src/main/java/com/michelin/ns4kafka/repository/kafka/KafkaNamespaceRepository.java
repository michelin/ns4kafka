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
import com.michelin.ns4kafka.repository.NamespaceRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/** Kafka namespace repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaNamespaceRepository extends KafkaStore<Namespace> implements NamespaceRepository {

    public KafkaNamespaceRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.namespaces") String kafkaTopic,
            @KafkaClient("namespace-producer") Producer<String, Namespace> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(Namespace namespace) {
        return namespace.getMetadata().getName();
    }

    @Override
    public Namespace createNamespace(Namespace namespace) {
        return produce(getMessageKey(namespace), namespace);
    }

    @Override
    public void delete(Namespace namespace) {
        produce(getMessageKey(namespace), null);
    }

    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.namespaces")
    void receive(ConsumerRecord<String, Namespace> message) {
        super.receive(message);
    }

    @Override
    public List<Namespace> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(namespace -> namespace.getMetadata().getCluster().equals(cluster))
                .toList();
    }

    @Override
    public Optional<Namespace> findByName(String namespace) {
        return getKafkaStore().values().stream()
                .filter(ns -> ns.getMetadata().getName().equals(namespace))
                .findFirst();
    }
}
