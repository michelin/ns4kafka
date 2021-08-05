package com.michelin.ns4kafka.logs;

import com.michelin.ns4kafka.controllers.ResourceController;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.scheduling.annotation.Async;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class KafkaLogListener implements ApplicationEventListener<ResourceController.AuditLog> {

    @Inject
    KafkaLogProducer kafkaProducer;

    @Override
    @Async
    public void onApplicationEvent(ResourceController.AuditLog event) {
        kafkaProducer.sendAuditLog(event.getKind()+"/"+event.getMetadata().getName() ,event);
    }


}

@KafkaClient
interface KafkaLogProducer {

    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.audit-logs")
    void sendAuditLog(@KafkaKey String key, ResourceController.AuditLog log);
}