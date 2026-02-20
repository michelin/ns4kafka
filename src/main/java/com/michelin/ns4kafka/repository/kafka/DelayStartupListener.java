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

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Singleton;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Delay startup listener. */
@Slf4j
@Singleton
public class DelayStartupListener implements ApplicationEventListener<StartupEvent> {
    private final List<KafkaStore<?>> kafkaStores;

    /**
     * Constructor.
     *
     * @param kafkaStores The Kafka stores
     */
    public DelayStartupListener(List<KafkaStore<?>> kafkaStores) {
        this.kafkaStores = kafkaStores;
    }

    /**
     * Wait for KafkaStores to be ready before starting the HTTP listener. This is required to avoid serving requests
     * before KafkaStores are ready.
     *
     * @param event the event to respond to
     */
    @Override
    public void onApplicationEvent(StartupEvent event) {
        while (!kafkaStores.stream().allMatch(KafkaStore::isInitialized)) {
            try {
                Thread.sleep(1000);
                log.info("Waiting for Kafka store to catch up");
            } catch (InterruptedException e) {
                log.error("Exception ", e);
                Thread.currentThread().interrupt();
            }
            kafkaStores.forEach(KafkaStore::reportInitProgress);
        }
    }
}
