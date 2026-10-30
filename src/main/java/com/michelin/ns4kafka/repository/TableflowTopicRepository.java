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

import com.michelin.ns4kafka.model.TableflowTopic;
import java.util.List;
import java.util.Optional;

/** Store Tableflow configurations and deployment status for the executor. */
public interface TableflowTopicRepository {
    /**
     * Find the stored configurations to process for a cluster.
     *
     * @param cluster The cluster
     * @return The list of Tableflow topics
     */
    List<TableflowTopic> findAllForCluster(String cluster);

    /**
     * Find a Tableflow topic by name and cluster.
     *
     * @param cluster The cluster
     * @param name The topic name
     * @return An optional topic
     */
    Optional<TableflowTopic> findByName(String cluster, String name);

    /**
     * Store a Tableflow configuration or its deployment status.
     *
     * @param topic The configuration to store
     * @return The stored configuration
     */
    TableflowTopic create(TableflowTopic topic);
}
