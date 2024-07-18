package com.michelin.ns4kafka.service;

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.query.RoleBindingFilterParams;
import com.michelin.ns4kafka.repository.RoleBindingRepository;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Service to manage role bindings.
 */
@Singleton
public class RoleBindingService {
    @Inject
    RoleBindingRepository roleBindingRepository;

    /**
     * Delete a role binding.
     *
     * @param roleBinding The role binding to delete
     */
    public void delete(RoleBinding roleBinding) {
        roleBindingRepository.delete(roleBinding);
    }

    /**
     * Create a role binding.
     *
     * @param roleBinding The role binding to create
     */
    public void create(RoleBinding roleBinding) {
        roleBindingRepository.create(roleBinding);
    }

    /**
     * List role bindings of a given namespace.
     *
     * @param namespace The namespace used to research
     * @return The list of associated role bindings
     */
    public List<RoleBinding> list(String namespace) {
        return roleBindingRepository.findAllForNamespace(namespace);
    }

    /**
     * List role bindings of a given namespace, filtered by given parameters.
     *
     * @param namespace The namespace used to research
     * @return The list of associated role bindings
     */
    public List<RoleBinding> list(String namespace, RoleBindingFilterParams params) {
        return roleBindingRepository.findAllForNamespace(namespace, params);
    }

    /**
     * List role bindings by groups.
     *
     * @param groups The groups used to research
     * @return The list of associated role bindings
     */
    public List<RoleBinding> listByGroups(Collection<String> groups) {
        return roleBindingRepository.findAllForGroups(groups);
    }

    /**
     * Find a role binding by name.
     *
     * @param namespace The namespace used to research
     * @param name      The role binding name
     * @return The researched role binding
     */
    public Optional<RoleBinding> findByName(String namespace, String name) {
        return list(namespace)
            .stream()
            .filter(t -> t.getMetadata().getName().equals(name))
            .findFirst();
    }
}
