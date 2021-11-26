package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import jakarta.inject.Singleton;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaRoleBindingRepository extends KafkaStore<RoleBinding> implements RoleBindingRepository {
    public KafkaRoleBindingRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.role-bindings") String kafkaTopic,
                                      @KafkaClient("role-binding-producer") Producer<String, RoleBinding> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(RoleBinding roleBinding) {
        return roleBinding.getMetadata().getNamespace() + "-" + roleBinding.getMetadata().getName();
    }

    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.role-bindings")
    void receive(ConsumerRecord<String, RoleBinding> record) {
        super.receive(record);
    }

    @Override
    public RoleBinding create(RoleBinding roleBinding) {
        return this.produce(getMessageKey(roleBinding),roleBinding);
    }

    @Override
    public void delete(RoleBinding roleBinding) {
        this.produce(getMessageKey(roleBinding),null);
    }

    @Override
    public List<RoleBinding> findAllForGroups(Collection<String> groups) {
        return getKafkaStore().values().stream().filter(roleBinding ->
                groups.stream().anyMatch(group ->
                        roleBinding.getSpec().getSubject().getSubjectType() == RoleBinding.SubjectType.GROUP
                                && roleBinding.getSpec().getSubject().getSubjectName().equals(group)
                )
        ).collect(Collectors.toList());

    }

    @Override
    public List<RoleBinding> findAllForNamespace(String namespace) {
        return getKafkaStore().values().stream()
                .filter(roleBinding -> roleBinding.getMetadata().getNamespace().equals(namespace))
                .collect(Collectors.toList());
    }

}
