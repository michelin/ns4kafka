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

import com.michelin.ns4kafka.model.Topic;
import java.util.List;

/** Topic repository. */
public interface TopicRepository {
    /**
     * Find all topics.
     *
     * @return The list of topics
     */
    List<Topic> findAll();

    /**
     * Find all topics by cluster.
     *
     * @param cluster The cluster
     * @return The list of topics
     */
    List<Topic> findAllForCluster(String cluster);

    /**
     * Create a given topic.
     *
     * @param topic The topic to create
     * @return The created topic
     */
    Topic create(Topic topic);

    /**
     * Delete a given topic.
     *
     * @param topic The topic to delete
     */
    void delete(Topic topic);
}
