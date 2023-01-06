package com.michelin.ns4kafka.services.executors;

import io.micronaut.runtime.event.ApplicationStartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
@Singleton
public class KafkaAsyncExecutorScheduler {
    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutors;

    @Inject
    List<AccessControlEntryAsyncExecutor> accessControlEntryAsyncExecutors;

    @Inject
    List<ConnectorAsyncExecutor> connectorAsyncExecutors;

    @Inject
    List<UserAsyncExecutor> userAsyncExecutors;

    private final AtomicBoolean ready = new AtomicBoolean(false);

    /**
     * Register when the application is ready
     * @param event The application start event
     */
    @EventListener
    public void onStartupEvent(ApplicationStartupEvent event) {
        ready.compareAndSet(false,true);
    }

    /**
     * Schedule resource synchronization
     */
    @Scheduled(initialDelay = "12s", fixedDelay = "20s")
    void schedule(){
        if (ready.get()) {
            topicAsyncExecutors.forEach(TopicAsyncExecutor::run);
            accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);
            connectorAsyncExecutors.forEach(ConnectorAsyncExecutor::run);
            userAsyncExecutors.forEach(UserAsyncExecutor::run);
        } else {
            log.warn("Scheduled jobs did not start because Micronaut is not ready yet");
        }
    }
}
