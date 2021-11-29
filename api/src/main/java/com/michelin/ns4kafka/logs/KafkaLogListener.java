package com.michelin.ns4kafka.logs;

import com.michelin.ns4kafka.models.AuditLog;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.core.util.StringUtils;
import io.micronaut.scheduling.annotation.Async;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "ns4kafka.log.kafka.enabled", value = StringUtils.TRUE)
public class KafkaLogListener implements ApplicationEventListener<AuditLog> {

    @Inject
    KafkaLogProducer kafkaProducer;

    @Override
    @Async
    public void onApplicationEvent(AuditLog event) {
        kafkaProducer.sendAuditLog(event.getMetadata().getNamespace(), event);
    }
}

@KafkaClient
@Requires(property = "ns4kafka.log.kafka.enabled", value = StringUtils.TRUE)
interface KafkaLogProducer {

    @Topic(value = "${ns4kafka.log.kafka.topic}")
    void sendAuditLog(@KafkaKey String namespace, AuditLog log);
}