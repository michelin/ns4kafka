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

import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.repository.StreamRepository;
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

/** Kafka Stream repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaStreamRepository extends KafkaStore<KafkaStream> implements StreamRepository {

    public KafkaStreamRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.streams") String kafkaTopic,
            @KafkaClient("streams-producer") Producer<String, KafkaStream> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(KafkaStream stream) {
        return stream.getMetadata().getCluster() + "/" + stream.getMetadata().getName();
    }

    @Override
    public List<KafkaStream> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(stream -> stream.getMetadata().getCluster().equals(cluster))
                .toList();
    }

    @Override
    public KafkaStream create(KafkaStream stream) {
        return this.produce(getMessageKey(stream), stream);
    }

    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.streams")
    void receive(ConsumerRecord<String, KafkaStream> message) {
        super.receive(message);
    }

    @Override
    public void delete(KafkaStream stream) {
        this.produce(getMessageKey(stream), null);
    }
}
