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

import com.michelin.ns4kafka.model.Namespace;
import java.util.List;
import java.util.Optional;

/** Namespace repository. */
public interface NamespaceRepository {
    /**
     * Find all namespaces for a given cluster.
     *
     * @param cluster The cluster name
     * @return The list of namespaces for the given cluster
     */
    List<Namespace> findAllForCluster(String cluster);

    /**
     * Find a namespace by name.
     *
     * @param namespace The namespace name
     * @return An optional containing the namespace if found, or empty if not found
     */
    Optional<Namespace> findByName(String namespace);

    /**
     * Create a namespace.
     *
     * @param namespace The namespace to create
     * @return The created namespace
     */
    Namespace create(Namespace namespace);

    /**
     * Delete a namespace.
     *
     * @param namespace The namespace to delete
     */
    void delete(Namespace namespace);
}
