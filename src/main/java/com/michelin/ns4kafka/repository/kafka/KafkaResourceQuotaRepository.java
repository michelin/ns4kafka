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

import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.ResourceQuotaRepository;
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

/** Kafka Resource Quota repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaResourceQuotaRepository extends KafkaStore<ResourceQuota> implements ResourceQuotaRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     * @param adminClient The Kafka admin client
     * @param ns4KafkaProperties Ns4Kafka properties
     * @param taskScheduler The task scheduler
     */
    public KafkaResourceQuotaRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.resource-quotas") String kafkaTopic,
            @KafkaClient("resource-quotas") Producer<String, ResourceQuota> kafkaProducer,
            AdminClient adminClient,
            Ns4KafkaProperties ns4KafkaProperties,
            @Named(TaskExecutors.SCHEDULED) TaskScheduler taskScheduler) {
        super(kafkaTopic, kafkaProducer, adminClient, ns4KafkaProperties, taskScheduler);
    }

    /**
     * Get the message key for a resource quota.
     *
     * @param message The resource quota message
     * @return The message key
     */
    @Override
    public String getMessageKey(ResourceQuota message) {
        return message.getMetadata().getNamespace();
    }

    /**
     * Find all quotas.
     *
     * @return The resource quotas
     */
    @Override
    public Collection<ResourceQuota> findAll() {
        return getKafkaStore().values();
    }

    /**
     * Get resource quota of a namespace.
     *
     * @param namespace The namespace used to research
     * @return A resource quota
     */
    @Override
    public Optional<ResourceQuota> findByNamespace(String namespace) {
        return Optional.ofNullable(getKafkaStore().get(namespace));
    }

    /**
     * Create a resource quota.
     *
     * @param resourceQuota The resource quota to create
     * @return The created resource quota
     */
    @Override
    public ResourceQuota create(ResourceQuota resourceQuota) {
        return produce(getMessageKey(resourceQuota), resourceQuota);
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

    /**
     * Consume messages from resource quotas topic.
     *
     * @param message The resource quota message
     */
    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.resource-quotas")
    public void receive(ConsumerRecord<String, ResourceQuota> message) {
        super.receive(message);
    }
}
