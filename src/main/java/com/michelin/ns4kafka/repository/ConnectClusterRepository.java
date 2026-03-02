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

import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;

import java.util.Collection;
import java.util.List;

/** Repository to manage Kafka Connect clusters. */
public interface ConnectClusterRepository {
    /**
     * Find all Kafka Connect clusters.
     *
     * @return The list of Kafka Connect clusters
     */
    Collection<ConnectCluster> findAll();

    /**
     * Find all Kafka Connect clusters for a given cluster.
     *
     * @param cluster The cluster name
     * @return The list of Kafka Connect clusters for the given cluster
     */
    List<ConnectCluster> findAllForCluster(String cluster);

    /**
     * Create a Kafka Connect cluster.
     *
     * @param connectCluster The Kafka Connect cluster to create
     * @return The created Kafka Connect cluster
     */
    ConnectCluster create(ConnectCluster connectCluster);

    /**
     * Delete a Kafka Connect cluster.
     *
     * @param connectCluster The Kafka Connect cluster to delete
     */
    void delete(ConnectCluster connectCluster);
}
