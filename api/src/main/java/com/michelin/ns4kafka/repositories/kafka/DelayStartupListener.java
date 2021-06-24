package com.michelin.ns4kafka.repositories.kafka;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;


@Slf4j
public class DelayStartupListener implements ApplicationEventListener<StartupEvent> {
    @Inject
    List<KafkaStore> kafkaStores;

    @Override
    public void onApplicationEvent(StartupEvent event) {
        // Micronaut will not start the HTTP listener until all ServerStartupEvent are completed
        // We must not serve requests if KafkaStores are not ready.
        while(!kafkaStores.stream().allMatch(kafkaStore -> kafkaStore.isInitialized()))
        {
            try {
                Thread.sleep(1000);
                log.info("Waiting for Kafka Stores to catch up");
            } catch (InterruptedException e) {
                log.error("Exception ",e);
                Thread.currentThread().interrupt();
            }
        }


    }
}
