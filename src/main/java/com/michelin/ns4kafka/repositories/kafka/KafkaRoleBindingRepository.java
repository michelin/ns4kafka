package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaRoleBindingRepository extends KafkaStore<RoleBinding> implements RoleBindingRepository {
    public KafkaRoleBindingRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.role-bindings") String kafkaTopic,
                                      @KafkaClient("role-binding-producer") Producer<String, RoleBinding> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(RoleBinding message) {
        return "temp";
    }

    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.role-bindings")
    void receive(ConsumerRecord<String, RoleBinding> record) {
        super.receive(record);
    }


    @Override
    public Collection<RoleBinding> findAllForGroups(Collection<String> groups) {
        return getKafkaStore().values().stream().filter(roleBinding ->
                groups.stream().anyMatch(group ->
                        roleBinding.getSubject().getSubjectType() == RoleBinding.SubjectType.GROUP
                                && roleBinding.getSubject().getSubjectName().equals(group)
                )
        ).collect(Collectors.toList());

    }

    @Override
    public RoleBinding create(RoleBinding roleBinding) {
        return this.produce(getMessageKey(roleBinding),roleBinding);
    }

}
