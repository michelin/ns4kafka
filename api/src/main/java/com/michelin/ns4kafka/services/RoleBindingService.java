package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Singleton
public class RoleBindingService {

    @Inject
    RoleBindingRepository roleBindingRepository;

    public void delete(RoleBinding roleBinding) {
        roleBindingRepository.delete(roleBinding);
    }

    public void create(RoleBinding rolebinding) {

        roleBindingRepository.create(rolebinding);
    }

    public List<RoleBinding> list(String namespace) {

        return roleBindingRepository.findAllForNamespace(namespace);
    }

    public Optional<RoleBinding> findByName(String namespace, String name) {
        return list(namespace)
                .stream()
                .filter(t -> t.getMetadata().getName().equals(name))
                .findFirst();
    }
}
