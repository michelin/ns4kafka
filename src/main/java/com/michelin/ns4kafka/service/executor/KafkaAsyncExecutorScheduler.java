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
package com.michelin.ns4kafka.service.executor;

import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import io.micronaut.runtime.event.ApplicationStartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

/** Schedule the asynchronous executors. */
@Slf4j
@Singleton
public class KafkaAsyncExecutorScheduler {
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final List<TopicAsyncExecutor> topicAsyncExecutors;
    private final List<AccessControlEntryAsyncExecutor> accessControlEntryAsyncExecutors;
    private final List<ConnectorAsyncExecutor> connectorAsyncExecutors;
    private final List<UserAsyncExecutor> userAsyncExecutors;
    private final Ns4KafkaProperties.SchedulerProperties schedulerProperties;

    /**
     * Constructor.
     *
     * @param topicAsyncExecutors The topic async executors
     * @param accessControlEntryAsyncExecutors The access control entry async executors
     * @param connectorAsyncExecutors The connector async executors
     * @param userAsyncExecutors The user async executors
     */
    public KafkaAsyncExecutorScheduler(
            List<TopicAsyncExecutor> topicAsyncExecutors,
            List<AccessControlEntryAsyncExecutor> accessControlEntryAsyncExecutors,
            List<ConnectorAsyncExecutor> connectorAsyncExecutors,
            List<UserAsyncExecutor> userAsyncExecutors,
            Ns4KafkaProperties.SchedulerProperties schedulerProperties) {
        this.topicAsyncExecutors = topicAsyncExecutors;
        this.accessControlEntryAsyncExecutors = accessControlEntryAsyncExecutors;
        this.connectorAsyncExecutors = connectorAsyncExecutors;
        this.userAsyncExecutors = userAsyncExecutors;
        this.schedulerProperties = schedulerProperties;
    }

    /**
     * Register when the application is ready.
     *
     * @param event The application start event
     */
    @EventListener
    public void onStartupEvent(ApplicationStartupEvent event) {
        ready.compareAndSet(false, true);
        scheduleConnectHealthCheck();
        scheduleConnectorSynchronization();
    }

    /** Schedule resource synchronization. */
    @Scheduled(initialDelay = "12s", fixedDelay = "20s")
    public void schedule() {
        if (ready.get()) {
            topicAsyncExecutors.forEach(TopicAsyncExecutor::run);
            accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);
            userAsyncExecutors.forEach(UserAsyncExecutor::run);
        } else {
            log.warn("Scheduled jobs did not start because Micronaut is not ready yet");
        }
    }

    /** Schedule connector synchronization. */
    public void scheduleConnectorSynchronization() {
        Flux.interval(
                        Duration.ofSeconds(12),
                        Duration.ofMillis(schedulerProperties.getConnector().getIntervalMs()))
                .onBackpressureDrop(
                        _ -> log.debug("Skipping next connector synchronization. The previous one is still running."))
                .concatMap(_ -> Flux.fromIterable(connectorAsyncExecutors).flatMap(ConnectorAsyncExecutor::run))
                .onErrorContinue((error, _) ->
                        log.trace("Continue connector synchronization after error: {}.", error.getMessage()))
                .subscribe(connectorInfo ->
                        log.trace("Synchronization completed for connector \"{}\".", connectorInfo.name()));
    }

    /** Schedule connector synchronization. */
    public void scheduleConnectHealthCheck() {
        Flux.interval(
                        Duration.ofSeconds(5),
                        Duration.ofMillis(schedulerProperties.getConnect().getIntervalMs()))
                .onBackpressureDrop(_ ->
                        log.debug("Skipping next Connect cluster health check. The previous one is still running."))
                .concatMap(
                        _ -> Flux.fromIterable(connectorAsyncExecutors).flatMap(ConnectorAsyncExecutor::runHealthCheck))
                .onErrorContinue((error, _) ->
                        log.trace("Continue Connect cluster health check after error: {}.", error.getMessage()))
                .subscribe(connectCluster -> log.trace(
                        "Health check completed for Connect cluster \"{}\".",
                        connectCluster.getMetadata().getName()));
    }
}
