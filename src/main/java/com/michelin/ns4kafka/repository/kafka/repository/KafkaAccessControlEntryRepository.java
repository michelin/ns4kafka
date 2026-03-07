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

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
import com.michelin.ns4kafka.repository.kafka.InternalTopic;
import com.michelin.ns4kafka.repository.kafka.KafkaStore;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;

/** Access control entry repository. */
@Singleton
public class KafkaAccessControlEntryRepository extends KafkaStore<AccessControlEntry>
        implements AccessControlEntryRepository {

    /**
     * Constructor.
     *
     * @param topicName The Kafka topic
     * @param kafkaProducer The Kafka producer
     */
    public KafkaAccessControlEntryRepository(
            KafkaStreams kafkaStreams,
            @Value("${ns4kafka.store.kafka.topics.prefix}." + InternalTopic.ACL) String topicName,
            @KafkaClient("access-control-entries-producer") Producer<String, AccessControlEntry> kafkaProducer) {
        super(kafkaStreams, topicName, kafkaProducer);
    }

    /**
     * Get the message key for an ACL.
     *
     * @param accessControlEntry The ACL
     * @return The message key
     */
    @Override
    public String getMessageKey(AccessControlEntry accessControlEntry) {
        return accessControlEntry.getMetadata().getNamespace() + "/"
                + accessControlEntry.getMetadata().getName();
    }

    /**
     * Find all ACLs.
     *
     * @return A collection of ACLs
     */
    @Override
    public Collection<AccessControlEntry> findAll() {
        List<AccessControlEntry> results = new ArrayList<>();
        try (KeyValueIterator<String, AccessControlEntry> iterator = queryAllStore()) {
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
     * Find an ACL by name.
     *
     * @param namespace The namespace of the ACL
     * @param name The name of the ACL
     * @return An optional containing the ACL if found, or empty if not found
     */
    @Override
    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return Optional.ofNullable(queryStoreByKey(namespace + "/" + name));
    }

    /**
     * Create an ACL.
     *
     * @param accessControlEntry The ACL to create
     */
    @Override
    public void create(AccessControlEntry accessControlEntry) {
        produce(getMessageKey(accessControlEntry), accessControlEntry);
    }

    /**
     * Delete an ACL.
     *
     * @param accessControlEntry The ACL to delete
     */
    @Override
    public void delete(AccessControlEntry accessControlEntry) {
        produce(getMessageKey(accessControlEntry), null);
    }
}
