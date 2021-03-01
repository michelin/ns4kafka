package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.RoleBinding;

import java.util.Collection;
import java.util.List;

public interface RoleBindingRepository {
    List<RoleBinding> findAllForGroups(Collection<String> groups);
    List<RoleBinding> findAllForNamespace(String namespace);
    RoleBinding create(RoleBinding roleBinding);
    void delete(RoleBinding roleBinding);
}
