package com.michelin.ns4kafka.repositories.kafka;

import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.ApplicationStartupEvent;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;


public class DelayStartupListener implements ApplicationEventListener<StartupEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(DelayStartupListener.class);
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
                LOG.info("Waiting for Kafka Stores to catch up");
            } catch (InterruptedException e) {
                LOG.error("Exception ",e);
            }
        }


    }
}
