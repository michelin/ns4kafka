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
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
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
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
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

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     * @param adminClient The Kafka admin client
     * @param ns4KafkaProperties Ns4Kafka properties
     * @param taskScheduler The task scheduler
     */
    public KafkaAccessControlEntryRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.access-control-entries") String kafkaTopic,
            @KafkaClient("access-control-entries-producer") Producer<String, AccessControlEntry> kafkaProducer,
            AdminClient adminClient,
            Ns4KafkaProperties ns4KafkaProperties,
            @Named(TaskExecutors.SCHEDULED) TaskScheduler taskScheduler) {
        super(kafkaTopic, kafkaProducer, adminClient, ns4KafkaProperties, taskScheduler);
    }

    /**
     * Get the message key for a given access control entry.
     *
     * @param accessControlEntry The message
     * @return The message key
     */
    @Override
    String getMessageKey(AccessControlEntry accessControlEntry) {
        return accessControlEntry.getMetadata().getNamespace() + "/"
                + accessControlEntry.getMetadata().getName();
    }

    /**
     * Find all access control entries.
     *
     * @return The collection of access control entries
     */
    @Override
    public Collection<AccessControlEntry> findAll() {
        return getKafkaStore().values();
    }

    /**
     * Find an access control entry by name.
     *
     * @param namespace The namespace
     * @param name The name
     * @return The access control entry if found, empty otherwise
     */
    @Override
    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return Optional.ofNullable(getKafkaStore().get(namespace + "/" + name));
    }

    /**
     * Create an access control entry.
     *
     * @param accessControlEntry The access control entry to create
     * @return The created access control entry
     */
    @Override
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        return produce(getMessageKey(accessControlEntry), accessControlEntry);
    }

    /**
     * Delete an access control entry.
     *
     * @param accessControlEntry The access control entry to delete
     */
    @Override
    public void delete(AccessControlEntry accessControlEntry) {
        produce(getMessageKey(accessControlEntry), null);
    }

    /**
     * Receive messages from the Kafka topic and update the local store accordingly.
     *
     * @param message The record
     */
    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.access-control-entries")
    public void receive(ConsumerRecord<String, AccessControlEntry> message) {
        super.receive(message);
    }
}
