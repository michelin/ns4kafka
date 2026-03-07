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
package com.michelin.ns4kafka.repository.kafka.streams;

import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.kafka.InternalTopic;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import io.micronaut.configuration.kafka.streams.event.BeforeKafkaStreamStart;
import io.micronaut.context.event.ApplicationEventListener;
import jakarta.inject.Singleton;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.KafkaStreams;

@Slf4j
@Singleton
public class KafkaStreamsStartListener implements ApplicationEventListener<BeforeKafkaStreamStart> {
    private final AdminClient adminClient;
    private final Ns4KafkaProperties ns4KafkaProperties;
    private final InternalTopic internalTopic;
    private final CountDownLatch latch;

    public KafkaStreamsStartListener(
            AdminClient adminClient, Ns4KafkaProperties ns4KafkaProperties, InternalTopic internalTopic) {
        this.adminClient = adminClient;
        this.ns4KafkaProperties = ns4KafkaProperties;
        this.internalTopic = internalTopic;
        this.latch = new CountDownLatch(1);
    }

    @Override
    public void onApplicationEvent(BeforeKafkaStreamStart event) {
        try {
            event.getKafkaStreams().setStateListener((newState, _) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            Set<String> allTopics = adminClient
                    .listTopics()
                    .names()
                    .get(ns4KafkaProperties.getStore().getKafka().getInitTimeout(), TimeUnit.MILLISECONDS);

            Set<String> existingTopics =
                    internalTopic.all().stream().filter(allTopics::contains).collect(Collectors.toSet());

            if (!existingTopics.isEmpty()) {
                verifyInternalTopic(existingTopics);
                return;
            }

            Set<String> missingTopics = internalTopic.all().stream()
                    .filter(topic -> !allTopics.contains(topic))
                    .collect(Collectors.toSet());

            createInternalTopic(missingTopics);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaStoreException(
                    "Thread interrupted trying to create or validate configuration of internal topics.", e);
        } catch (ExecutionException e) {
            throw new KafkaStoreException(
                    "Execution error trying to create or validate configuration of internal topics.", e);
        } catch (TimeoutException e) {
            throw new KafkaStoreException(
                    "Timed out trying to create or validate configuration of internal topics.", e);
        }
    }

    private void verifyInternalTopic(Set<String> existingTopics)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, TopicDescription> topicDescription = adminClient
                .describeTopics(existingTopics)
                .allTopicNames()
                .get(ns4KafkaProperties.getStore().getKafka().getInitTimeout(), TimeUnit.MILLISECONDS);

        for (Map.Entry<String, TopicDescription> entry : topicDescription.entrySet()) {
            final int numPartitions = entry.getValue().partitions().size();
            if (numPartitions != 1) {
                throw new KafkaStoreException(
                        "The topic " + entry.getKey() + " should have only 1 partition but has " + numPartitions + ".");
            }

            if (entry.getValue().partitions().getFirst().replicas().size()
                            < ns4KafkaProperties
                                    .getStore()
                                    .getKafka()
                                    .getTopics()
                                    .getReplicationFactor()
                    && log.isWarnEnabled()) {
                log.warn(
                        "The replication factor of the topic {} is less than the desired one of {}. If this is a production environment, it's crucial to add more brokers and increase the replication factor of the topic.",
                        entry.getKey(),
                        ns4KafkaProperties.getStore().getKafka().getTopics().getReplicationFactor());
            }

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, entry.getKey());
            Map<ConfigResource, Config> configs = adminClient
                    .describeConfigs(Collections.singleton(topicResource))
                    .all()
                    .get(ns4KafkaProperties.getStore().getKafka().getInitTimeout(), TimeUnit.MILLISECONDS);

            Config topicConfigs = configs.get(topicResource);
            String retentionPolicy =
                    topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();

            if (!TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
                if (log.isErrorEnabled()) {
                    log.error(
                            "The retention policy of the topic {} is incorrect. You must configure the topic to 'compact' cleanup policy to avoid Kafka deleting your data after a week. Refer to Kafka documentation for more details on cleanup policies.",
                            entry.getKey());
                }

                throw new KafkaStoreException("The retention policy of the topic " + entry.getKey()
                        + " is incorrect. Expected cleanup.policy to be 'compact' but it is " + retentionPolicy);
            }
        }
    }

    private void createInternalTopic(Set<String> missingTopics)
            throws InterruptedException, ExecutionException, TimeoutException {
        int numLiveBrokers = adminClient
                .describeCluster()
                .nodes()
                .get(ns4KafkaProperties.getStore().getKafka().getInitTimeout(), TimeUnit.MILLISECONDS)
                .size();

        if (numLiveBrokers == 0) {
            throw new KafkaStoreException("No live Kafka brokers.");
        }

        int replicationFactor = Math.min(
                numLiveBrokers,
                ns4KafkaProperties.getStore().getKafka().getTopics().getReplicationFactor());
        if (replicationFactor
                        < ns4KafkaProperties.getStore().getKafka().getTopics().getReplicationFactor()
                && log.isWarnEnabled()) {
            log.warn(
                    "Creating topic using a replication factor of {}, which is less than the desired one of {}. If this is a production environment, it's crucial to add more brokers and increase the replication factor of the topic.",
                    replicationFactor,
                    ns4KafkaProperties.getStore().getKafka().getTopics().getReplicationFactor());
        }

        List<NewTopic> newTopics = missingTopics.stream()
                .map(topic -> {
                    NewTopic topicRequest = new NewTopic(topic, 1, (short) replicationFactor);
                    topicRequest.configs(
                            ns4KafkaProperties.getStore().getKafka().getTopics().getProps());
                    return topicRequest;
                })
                .toList();

        adminClient
                .createTopics(newTopics)
                .all()
                .get(ns4KafkaProperties.getStore().getKafka().getInitTimeout(), TimeUnit.MILLISECONDS);
    }

    public void await() throws InterruptedException {
        latch.await();
    }
}
