package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Namespace;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.reactivex.Single;
import org.apache.kafka.clients.producer.RecordMetadata;

@KafkaClient
public interface KafkaProducer {

    @Topic("${ns4kafka.store.kafka.topics.namespaces}")
    RecordMetadata writeNOOPtoNamespaceTopic(
            @KafkaKey String noop,
            String message
    );
}
