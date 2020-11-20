package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.role.RoleBinding;

import javax.management.relation.Role;
import java.util.Collection;

public interface RoleBindingRepository {
    Collection<RoleBinding> findAllForGroups(Collection<String> groups);
    RoleBinding create(RoleBinding roleBinding);
}
