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

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.repository.RoleBindingRepository;
import com.michelin.ns4kafka.repository.kafka.InternalTopic;
import com.michelin.ns4kafka.repository.kafka.KafkaStore;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;

/** Kafka Role Binding repository. */
@Singleton
public class KafkaRoleBindingRepository extends KafkaStore<RoleBinding> implements RoleBindingRepository {

    /**
     * Constructor.
     *
     * @param kafkaTopic The Kafka topic
     * @param kafkaProducer The Kafka producer
     */
    public KafkaRoleBindingRepository(
            KafkaStreams kafkaStreams,
            @Value("${ns4kafka.store.kafka.topics.prefix}." + InternalTopic.ROLE_BINDING) String kafkaTopic,
            @KafkaClient("role-binding-producer") Producer<String, RoleBinding> kafkaProducer) {
        super(kafkaStreams, kafkaTopic, kafkaProducer);
    }

    /**
     * Build message key from role binding.
     *
     * @param roleBinding The role binding used to build the key
     * @return A key
     */
    @Override
    public String getMessageKey(RoleBinding roleBinding) {
        return roleBinding.getMetadata().getNamespace() + "-"
                + roleBinding.getMetadata().getName();
    }

    /**
     * List role bindings by groups.
     *
     * @param groups The groups used to research
     * @return The list of associated role bindings
     */
    @Override
    public List<RoleBinding> findAllForGroups(Collection<String> groups) {
        List<RoleBinding> results = new ArrayList<>();
        try (KeyValueIterator<String, RoleBinding> iterator = queryAllStore()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (entry.value != null
                        && groups.stream()
                                .anyMatch(group ->
                                        entry.value.getSpec().getSubject().getSubjectType()
                                                        == RoleBinding.SubjectType.GROUP
                                                && entry.value
                                                        .getSpec()
                                                        .getSubject()
                                                        .getSubjectName()
                                                        .equalsIgnoreCase(group))) {
                    results.add(entry.value);
                }
            }
        }
        return results;
    }

    /**
     * List role bindings by namespace.
     *
     * @param namespace The namespace used to research
     * @return The list of associated role bindings
     */
    @Override
    public List<RoleBinding> findAllForNamespace(String namespace) {
        List<RoleBinding> results = new ArrayList<>();
        try (KeyValueIterator<String, RoleBinding> iterator = queryAllStore()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (entry.value != null
                        && entry.value.getMetadata().getNamespace().equals(namespace)) {
                    results.add(entry.value);
                }
            }
        }
        return results;
    }

    /**
     * Create a role binding.
     *
     * @param roleBinding The role binding to create
     */
    @Override
    public void create(RoleBinding roleBinding) {
        this.produce(getMessageKey(roleBinding), roleBinding);
    }

    /**
     * Delete a role binding.
     *
     * @param roleBinding The role binding to delete
     */
    @Override
    public void delete(RoleBinding roleBinding) {
        this.produce(getMessageKey(roleBinding), null);
    }
}
