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
package com.michelin.ns4kafka.controller.connect;

import com.michelin.ns4kafka.controller.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.ConnectClusterService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import reactor.core.publisher.Flux;

/** Non-namespaced controller to manage Kafka Connect clusters. */
@Tag(name = "Connect Clusters", description = "Manage the Kafka Connect clusters.")
@Controller(value = "/api/connect-clusters")
@ExecuteOn(TaskExecutors.IO)
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class ConnectClusterNonNamespacedController extends NonNamespacedResourceController {
    @Inject
    private ConnectClusterService connectClusterService;

    /**
     * List Kafka Connect clusters.
     *
     * @return A list of Kafka Connect clusters
     */
    @Get("{?all}")
    public Flux<ConnectCluster> listAll(@QueryValue(defaultValue = "false") boolean all) {
        return connectClusterService.findAll(all);
    }
}
