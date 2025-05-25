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

import io.micronaut.runtime.event.ApplicationStartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
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

    @Inject
    private List<TopicAsyncExecutor> topicAsyncExecutors;

    @Inject
    private List<AccessControlEntryAsyncExecutor> accessControlEntryAsyncExecutors;

    @Inject
    private List<ConnectorAsyncExecutor> connectorAsyncExecutors;

    @Inject
    private List<UserAsyncExecutor> userAsyncExecutors;

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
        Flux.interval(Duration.ofSeconds(12), Duration.ofSeconds(30))
                .onBackpressureDrop(onDropped ->
                        log.debug("Skipping next connector synchronization. The previous one is still running."))
                .concatMap(mapper -> Flux.fromIterable(connectorAsyncExecutors).flatMap(ConnectorAsyncExecutor::run))
                .onErrorContinue((error, body) ->
                        log.trace("Continue connector synchronization after error: {}.", error.getMessage()))
                .subscribe(connectorInfo ->
                        log.trace("Synchronization completed for connector \"{}\".", connectorInfo.name()));
    }

    /** Schedule connector synchronization. */
    public void scheduleConnectHealthCheck() {
        Flux.interval(Duration.ofSeconds(5), Duration.ofMinutes(1))
                .onBackpressureDrop(onDropped ->
                        log.debug("Skipping next Connect cluster health check. The previous one is still running."))
                .concatMap(mapper ->
                        Flux.fromIterable(connectorAsyncExecutors).flatMap(ConnectorAsyncExecutor::runHealthCheck))
                .onErrorContinue((error, body) ->
                        log.trace("Continue Connect cluster health check after error: {}.", error.getMessage()))
                .subscribe(connectCluster -> log.trace(
                        "Health check completed for Connect cluster \"{}\".",
                        connectCluster.getMetadata().getName()));
    }
}
