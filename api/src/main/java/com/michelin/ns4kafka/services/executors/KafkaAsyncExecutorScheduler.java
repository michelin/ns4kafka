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
     * Asynchronous executor for topics
     */
    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutors;

    /**
     * Asynchronous executor for ACLs
     */
    @Inject
    List<AccessControlEntryAsyncExecutor> accessControlEntryAsyncExecutors;

    /**
     * Asynchronous executor for connectors
     */
    @Inject
    List<ConnectorAsyncExecutor> connectorAsyncExecutors;

    /**
     * Asynchronous executor for schemas
     */
    @Inject
    List<SubjectAsyncExecutor> subjectAsyncExecutors;

    private final AtomicBoolean ready = new AtomicBoolean(false);

    @EventListener
    public void onStartupEvent(ApplicationStartupEvent event) {
        // startup logic here
        ready.compareAndSet(false,true);
    }

    //TODO urgent : start the schedulder only when Application is started (ServerStartupEvent)
    @Scheduled(initialDelay = "12s", fixedDelay = "20s")
    void schedule(){

        if(ready.get()) {
            //TODO sequential forEach with exception handling (to let next clusters sync)
            topicAsyncExecutors.forEach(TopicAsyncExecutor::run);
            accessControlEntryAsyncExecutors.forEach(AccessControlEntryAsyncExecutor::run);
            connectorAsyncExecutors.forEach(ConnectorAsyncExecutor::run);
            subjectAsyncExecutors.forEach(SubjectAsyncExecutor::run);
        }else {
            log.warn("Scheduled job did not start because micronaut is not ready yet");
        }
    }
}
