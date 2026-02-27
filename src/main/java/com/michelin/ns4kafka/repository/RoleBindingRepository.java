/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.repository;

import com.michelin.ns4kafka.model.RoleBinding;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Role binding repository. */
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
     * Find a role binding by name.
     *
     * @param namespace The namespace
     * @param name The name
     * @return The role binding
     */
    Optional<RoleBinding> findByName(String namespace, String name);

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
