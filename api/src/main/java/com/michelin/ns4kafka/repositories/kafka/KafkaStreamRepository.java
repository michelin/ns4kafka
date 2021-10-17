package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.KafkaStream;
import com.michelin.ns4kafka.repositories.StreamRepository;
import io.micronaut.context.annotation.Value;

import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class KafkaStreamRepository extends KafkaStore<KafkaStream> implements StreamRepository {

    public KafkaStreamRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.streams") String kafkaTopic) {
        super(kafkaTopic);
    }

    @Override
    String getMessageKey(KafkaStream stream) {
        return stream.getMetadata().getCluster()+"/"+ stream.getMetadata().getName();
    }

    @Override
    public List<KafkaStream> findAllForCluster(String cluster) {
        return getKafkaStore().values()
                .stream()
                .filter(stream -> stream.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

    @Override
    public KafkaStream create(KafkaStream stream) {
        return this.produce(getMessageKey(stream), stream);
    }

    @Override
    public void delete(KafkaStream stream) {
        this.produce(getMessageKey(stream),null);
    }

}
