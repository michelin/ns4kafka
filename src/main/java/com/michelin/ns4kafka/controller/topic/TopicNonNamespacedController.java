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
package com.michelin.ns4kafka.controller.topic;

import com.michelin.ns4kafka.controller.generic.ResourceController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Resource.Metadata.Phase;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.TopicService;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.utils.SecurityService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import java.util.Collection;
import org.jspecify.annotations.Nullable;

/** Non namespaced controller for topics. */
@Tag(name = "Topics", description = "Manage the topics.")
@Controller(value = "/api/topics")
@ExecuteOn(TaskExecutors.IO)
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class TopicNonNamespacedController extends ResourceController {
    private final TopicService topicService;

    /**
     * Constructor.
     *
     * @param topicService The topic service
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    protected TopicNonNamespacedController(
            TopicService topicService,
            SecurityService securityService,
            ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        super(securityService, applicationEventPublisher);
        this.topicService = topicService;
    }

    /**
     * List topics, filtered by the given metadata status phase.
     *
     * @param phase The phase filter. If not provided, all topics are returned
     * @return A list of topics
     */
    @Get
    public Collection<Topic> listAll(@QueryValue @Nullable Phase phase) {
        return topicService.findAllByPhase(phase);
    }
}
