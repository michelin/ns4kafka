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
package com.michelin.ns4kafka.controller.acl;

import com.michelin.ns4kafka.controller.generic.ResourceController;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.AclService;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.utils.SecurityService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import java.util.List;

/** Non-namespaced controller to manage ACLs. */
@Tag(name = "ACLs", description = "Manage the ACLs.")
@Controller("/api/acls")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class AclNonNamespacedController extends ResourceController {
    private final AclService aclService;

    /**
     * Constructor.
     *
     * @param aclService The ACL service
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    public AclNonNamespacedController(
            AclService aclService,
            SecurityService securityService,
            ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        super(securityService, applicationEventPublisher);
        this.aclService = aclService;
    }

    /**
     * List ACLs.
     *
     * @return A list of ACLs
     */
    @Get
    public List<AccessControlEntry> listAll() {
        return aclService.findAll();
    }
}
