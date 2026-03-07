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

import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.repository.ResourceQuotaRepository;
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

/** Kafka Resource Quota repository. */
@Singleton
public class KafkaResourceQuotaRepository extends KafkaStore<ResourceQuota> implements ResourceQuotaRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     */
    public KafkaResourceQuotaRepository(
            KafkaStreams kafkaStreams,
            @Value("${ns4kafka.store.kafka.topics.prefix}." + InternalTopic.RESOURCE_QUOTA) String kafkaTopic,
            @KafkaClient("resource-quotas") Producer<String, ResourceQuota> kafkaProducer) {
        super(kafkaStreams, kafkaTopic, kafkaProducer);
    }

    /**
     * Get the message key for a resource quota message, which is the namespace.
     *
     * @param message The resource quota message
     * @return The message key
     */
    @Override
    public String getMessageKey(ResourceQuota message) {
        return message.getMetadata().getNamespace();
    }

    /**
     * Find all quotas of all namespaces.
     *
     * @return The resource quotas
     */
    @Override
    public Collection<ResourceQuota> findAll() {
        List<ResourceQuota> results = new ArrayList<>();
        try (KeyValueIterator<String, ResourceQuota> iterator = queryAllStore()) {
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
     * Get resource quota of a given namespace.
     *
     * @param namespace The namespace used to research
     * @return A resource quota
     */
    @Override
    public Optional<ResourceQuota> findByNamespace(String namespace) {
        return Optional.ofNullable(queryStoreByKey(namespace));
    }

    /**
     * Create a resource quota.
     *
     * @param resourceQuota The resource quota to create
     */
    @Override
    public void create(ResourceQuota resourceQuota) {
        produce(getMessageKey(resourceQuota), resourceQuota);
    }

    /**
     * Delete a resource quota.
     *
     * @param resourceQuota The resource quota to delete
     */
    @Override
    public void delete(ResourceQuota resourceQuota) {
        produce(getMessageKey(resourceQuota), null);
    }
}
