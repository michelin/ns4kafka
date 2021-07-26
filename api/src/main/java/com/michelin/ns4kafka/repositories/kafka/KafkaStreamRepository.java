package com.michelin.ns4kafka.repositories.kafka;

import java.util.List;

import javax.inject.Singleton;

import com.michelin.ns4kafka.models.KafkaStream;
import com.michelin.ns4kafka.repositories.StreamRepository;

import org.apache.kafka.clients.producer.Producer;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaStreamRepository extends KafkaStore<KafkaStream> implements StreamRepository {

    public KafkaStreamRepository(String kafkaTopic, Producer<String, KafkaStream> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
        //TODO Auto-generated constructor stub
    }

    @Override
    public List<KafkaStream> findAllForCluster(String cluster) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public KafkaStream create(KafkaStream topic) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(KafkaStream topic) {
        // TODO Auto-generated method stub

    }

    @Override
    String getMessageKey(KafkaStream message) {
        // TODO Auto-generated method stub
        return null;
    }
}
