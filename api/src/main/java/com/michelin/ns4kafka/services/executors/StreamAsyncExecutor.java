package com.michelin.ns4kafka.services.executors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.repositories.StreamRepository;

import org.apache.kafka.clients.admin.Admin;

import io.micronaut.context.annotation.EachBean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class StreamAsyncExecutor {

    private KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;
    @Inject
    StreamRepository streamRepository;

    private Admin getAdminClient(){
        return kafkaAsyncExecutorConfig.getAdminClient();
    }

    public void run(){
        if (kafkaAsyncExecutorConfig.isManageStreams()){
            synchronizeStream();
        }
    }

    public void synchronizeStream(){
        log.debug("Starting stream collection for cluster {}", kafkaAsyncExecutorConfig.getName());

    }

}
