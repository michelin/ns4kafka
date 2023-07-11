package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorInfo;
import io.micronaut.runtime.event.ApplicationStartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

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
        scheduleConnectorSynchronization();
    }

    /**
     * Schedule resource synchronization
     */
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

    /**
     * Schedule connector synchronization
     */
    public void scheduleConnectorSynchronization() {
        Flux.interval(Duration.ofSeconds(12), Duration.ofSeconds(30))
                .onBackpressureDrop(onDropped -> log.debug("Skipping next connector synchronization. The previous one is still running."))
                .concatMap(mapper -> {
                    List<Flux<ConnectorInfo>> clusterSyncResponses = connectorAsyncExecutors
                            .stream()
                            .map(ConnectorAsyncExecutor::run)
                            .toList();
                    return Flux.fromIterable(clusterSyncResponses).flatMap(Function.identity());
                })
                .subscribe();
    }
}
