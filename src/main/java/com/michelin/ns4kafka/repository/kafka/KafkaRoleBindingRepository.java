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

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.repository.RoleBindingRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/** Kafka role binding repository. */
@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED)
public class KafkaRoleBindingRepository extends KafkaStore<RoleBinding> implements RoleBindingRepository {
    /**
     * Constructor.
     *
     * @param kafkaTopic The role bindings topic
     * @param kafkaProducer The role bindings kafka producer
     */
    public KafkaRoleBindingRepository(
            @Value("${ns4kafka.store.kafka.topics.prefix}.role-bindings") String kafkaTopic,
            @KafkaClient("role-binding-producer") Producer<String, RoleBinding> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    /**
     * Build message key from role binding.
     *
     * @param roleBinding The role binding used to build the key
     * @return A key
     */
    @Override
    String getMessageKey(RoleBinding roleBinding) {
        return roleBinding.getMetadata().getNamespace() + "-"
                + roleBinding.getMetadata().getName();
    }

    /**
     * Consume messages from role bindings topic.
     *
     * @param message The role binding message
     */
    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.role-bindings")
    void receive(ConsumerRecord<String, RoleBinding> message) {
        super.receive(message);
    }

    /**
     * Produce a role binding message.
     *
     * @param roleBinding The role binding to create
     * @return The created role binding
     */
    @Override
    public RoleBinding create(RoleBinding roleBinding) {
        return this.produce(getMessageKey(roleBinding), roleBinding);
    }

    /**
     * Delete a role binding message by pushing a tomb stone message.
     *
     * @param roleBinding The role binding to delete
     */
    @Override
    public void delete(RoleBinding roleBinding) {
        this.produce(getMessageKey(roleBinding), null);
    }

    /**
     * List role bindings by groups.
     *
     * @param groups The groups used to research
     * @return The list of associated role bindings
     */
    @Override
    public List<RoleBinding> findAllForGroups(Collection<String> groups) {
        return getKafkaStore().values().stream()
                .filter(roleBinding -> groups.stream()
                        .anyMatch(group ->
                                roleBinding.getSpec().getSubject().getSubjectType() == RoleBinding.SubjectType.GROUP
                                        && roleBinding
                                                .getSpec()
                                                .getSubject()
                                                .getSubjectName()
                                                .equals(group)))
                .toList();
    }

    /**
     * List role bindings by namespace.
     *
     * @param namespace The namespace used to research
     * @return The list of associated role bindings
     */
    @Override
    public List<RoleBinding> findAllForNamespace(String namespace) {
        return getKafkaStore().values().stream()
                .filter(roleBinding -> roleBinding.getMetadata().getNamespace().equals(namespace))
                .toList();
    }
}
