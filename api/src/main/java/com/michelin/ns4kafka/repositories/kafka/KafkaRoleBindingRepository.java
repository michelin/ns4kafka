package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.context.annotation.Value;

import javax.inject.Singleton;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class KafkaRoleBindingRepository extends KafkaStore<RoleBinding> implements RoleBindingRepository {
    public KafkaRoleBindingRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.role-bindings") String kafkaTopic) {
        super(kafkaTopic);
    }

    @Override
    String getMessageKey(RoleBinding roleBinding) {
        return roleBinding.getMetadata().getNamespace() + "-" + roleBinding.getMetadata().getName();
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
