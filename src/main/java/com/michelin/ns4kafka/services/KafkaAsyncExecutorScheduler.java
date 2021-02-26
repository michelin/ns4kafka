package com.michelin.ns4kafka.services;

import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;


@Singleton
public class KafkaAsyncExecutorScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAsyncExecutorScheduler.class);

    @Inject
    List<KafkaAsyncExecutor> kafkaAsyncExecutors;

    //TODO urgent : start the schedulder only when Application is started (ServerStartupEvent)
    @Scheduled(initialDelay = "12s", fixedDelay = "20s")
    void schedule(){

        //TODO sequential forEach with exception handling (to let next clusters sync)
        kafkaAsyncExecutors.forEach(KafkaAsyncExecutor::run);
    }
}
