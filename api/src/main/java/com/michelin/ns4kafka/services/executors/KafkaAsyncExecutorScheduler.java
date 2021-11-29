package com.michelin.ns4kafka.services.executors;

import io.micronaut.runtime.event.ApplicationStartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
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
        }else {
            log.warn("Scheduled job did not start because micronaut is not ready yet");
        }
    }
}
