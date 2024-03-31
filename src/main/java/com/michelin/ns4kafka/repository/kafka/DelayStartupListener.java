package com.michelin.ns4kafka.repository.kafka;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import jakarta.inject.Inject;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Delay startup listener.
 */
@Slf4j
public class DelayStartupListener implements ApplicationEventListener<StartupEvent> {
    @Inject
    List<KafkaStore<?>> kafkaStores;

    /**
     * Wait for KafkaStores to be ready before starting the HTTP listener.
     * This is required to avoid serving requests before KafkaStores are ready.
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
