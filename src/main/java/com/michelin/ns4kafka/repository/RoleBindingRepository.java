package com.michelin.ns4kafka.repository;

import com.michelin.ns4kafka.model.RoleBinding;
import java.util.Collection;
import java.util.List;

/**
 * Role binding repository.
 */
public interface RoleBindingRepository {
    /**
     * List role bindings by groups.
     *
     * @param groups The groups used to research
     * @return The list of associated role bindings
     */
    List<RoleBinding> findAllForGroups(Collection<String> groups);

    /**
     * List role bindings by namespace.
     *
     * @param namespace The namespace used to research
     * @return The list of associated role bindings
     */
    List<RoleBinding> findAllForNamespace(String namespace);

    /**
     * Create a role binding.
     *
     * @param roleBinding The role binding to create
     */
    RoleBinding create(RoleBinding roleBinding);

    /**
     * Delete a role binding.
     *
     * @param roleBinding The role binding to delete
     */
    void delete(RoleBinding roleBinding);
}
