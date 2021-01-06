package com.michelin.ns4kafka.services;

import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;


@Singleton
public class KafkaAsyncExecutorScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAsyncExecutorScheduler.class);

    @Inject
    ApplicationContext applicationContext;

    @Scheduled(initialDelay = "12s", fixedDelay = "20s")
    void schedule(){
        Collection<KafkaAsyncExecutor> firstConfig = applicationContext.getBeansOfType(KafkaAsyncExecutor.class);
        firstConfig.forEach(KafkaAsyncExecutor::collect);
    }
}
