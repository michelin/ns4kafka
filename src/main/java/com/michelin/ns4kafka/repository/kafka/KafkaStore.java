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

import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

/**
 * Kafka store.
 *
 * @param <T> The type of the store
 */
@Slf4j
public abstract class KafkaStore<T> {
    private final KafkaStreams kafkaStreams;
    private final String topicName;
    private final Producer<String, T> producer;
    private final Ns4KafkaProperties ns4KafkaProperties;

    protected KafkaStore(KafkaStreams kafkaStreams, String topicName, Producer<String, T> producer, Ns4KafkaProperties ns4KafkaProperties) {
        this.kafkaStreams = kafkaStreams;
        this.topicName = topicName;
        this.producer = producer;
        this.ns4KafkaProperties = ns4KafkaProperties;
    }

    protected KeyValueIterator<String, T> queryAllStore() {
        return kafkaStreams
                .query(StateQueryRequest.inStore(topicName).withQuery(RangeQuery.<String, T>withNoBounds()))
                .getOnlyPartitionResult()
                .getResult();
    }

    protected T queryStoreByKey(String key) {
        QueryResult<T> result = kafkaStreams
                .query(StateQueryRequest.inStore(topicName).withQuery(KeyQuery.<String, T>withKey(key)))
                .getOnlyPartitionResult();

        if (result == null)    {
            return null;
        }

        return result.getResult();
    }

    protected void produce(String key, T message) throws KafkaStoreException {
        if (key == null) {
            throw new KafkaStoreException("Key should not be null");
        }

        try {
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(topicName, key, message);
            producer.send(producerRecord).get(ns4KafkaProperties.getStore().getKafka().getInitTimeout(), TimeUnit.MILLISECONDS);
            waitUntilStoreIsUpdated(key, message);
        } catch (Exception e) {
            throw new KafkaStoreException("Failed to produce message to Kafka store", e);
        }
    }

    private void waitUntilStoreIsUpdated(String key, T message) throws InterruptedException {
        long timeoutMs = ns4KafkaProperties.getStore().getKafka().getInitTimeout();
        long deadline = System.currentTimeMillis() + timeoutMs;


        // TODO: How to wait until the store is updated :/
    }

    /**
     * Get message key.
     *
     * @param message The message
     * @return The key of the message
     */
    protected abstract String getMessageKey(T message);
}
