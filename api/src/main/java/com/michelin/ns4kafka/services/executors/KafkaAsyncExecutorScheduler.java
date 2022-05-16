package com.michelin.ns4kafka.services.executors;

import io.micronaut.runtime.event.ApplicationStartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
@Singleton
public class KafkaAsyncExecutorScheduler {
    /**
     * The topic async executor
     */
    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutors;

    /**
     * The ACL async executor
     */
    @Inject
    List<AccessControlEntryAsyncExecutor> accessControlEntryAsyncExecutors;

    /**
     * The connector async executor
     */
    @Inject
    List<ConnectorAsyncExecutor> connectorAsyncExecutors;

    /**
     * The user async executor
     */
    @Inject
    List<UserAsyncExecutor> userAsyncExecutors;

    /**
     * Is the application ready
     */
    private final AtomicBoolean ready = new AtomicBoolean(false);

    /**
     * Register when the application is ready
     * @param event The application start event
     */
    @EventListener
    public void onStartupEvent(ApplicationStartupEvent event) {
        ready.compareAndSet(false,true);
    }

    @Scheduled(initialDelay = "12s", fixedDelay = "20s")
    void schedule(){
        if (ready.get()) {
            //TODO sequential forEach with exception handling (to let next clusters sync)
            topicAsyncExecutors.forEach(TopicAsyncExecutor::run);
            accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);
            connectorAsyncExecutors.forEach(ConnectorAsyncExecutor::run);
            userAsyncExecutors.forEach(UserAsyncExecutor::run);
        } else {
            log.warn("Scheduled jobs did not start because Micronaut is not ready yet");
        }
    }
}
