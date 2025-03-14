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

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.Collection;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/** Access control entry repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaAccessControlEntryRepository extends KafkaStore<AccessControlEntry>
        implements AccessControlEntryRepository {
    public KafkaAccessControlEntryRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.access-control-entries") String kafkaTopic,
            @KafkaClient("access-control-entries-producer") Producer<String, AccessControlEntry> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(AccessControlEntry accessControlEntry) {
        return accessControlEntry.getMetadata().getNamespace() + "/"
                + accessControlEntry.getMetadata().getName();
    }

    @Override
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        return this.produce(getMessageKey(accessControlEntry), accessControlEntry);
    }

    @Override
    public void delete(AccessControlEntry accessControlEntry) {
        produce(getMessageKey(accessControlEntry), null);
    }

    @Override
    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return getKafkaStore().values().stream()
                .filter(ace -> ace.getMetadata().getNamespace().equals(namespace))
                .filter(ace -> ace.getMetadata().getName().equals(name))
                .findFirst();
    }

    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.access-control-entries")
    void receive(ConsumerRecord<String, AccessControlEntry> message) {
        super.receive(message);
    }

    @Override
    public Collection<AccessControlEntry> findAll() {
        return getKafkaStore().values();
    }
}
