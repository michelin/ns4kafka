package com.michelin.ns4kafka.services;

import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.List;


@Singleton
public class KafkaAsyncExecutorScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAsyncExecutorScheduler.class);

    @Inject
    ApplicationContext applicationContext;
    @Inject
    List<ConnectRestService> connectRestServices;
    @Inject
    List<KafkaAsyncExecutor> kafkaAsyncExecutors;

    @Scheduled(initialDelay = "12s", fixedDelay = "20s")
    void schedule(){

        kafkaAsyncExecutors.forEach(KafkaAsyncExecutor::run);
    }
}
