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

import com.michelin.ns4kafka.model.quota.ResourceQuota;
import java.util.Optional;

/** Resource quota repository. */
public interface ResourceQuotaRepository {

    /**
     * Get resource quota by namespace.
     *
     * @param namespace The namespace used to research
     * @return The resource quotas associated to the namespace
     */
    Optional<ResourceQuota> findForNamespace(String namespace);

    /**
     * Create a resource quota.
     *
     * @param resourceQuota The resource quota to create
     */
    ResourceQuota create(ResourceQuota resourceQuota);

    /**
     * Delete a resource quota.
     *
     * @param resourceQuota The resource quota to delete
     */
    void delete(ResourceQuota resourceQuota);
}
