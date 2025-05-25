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
package com.michelin.ns4kafka.controller;

import com.michelin.ns4kafka.model.AppInfo;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;

@Tag(name = "Version", description = "Get the version.")
@RolesAllowed(SecurityRule.IS_ANONYMOUS)
@Controller(value = "/api/app-info")
public class AppInfoController {
    @Inject
    private Ns4KafkaProperties ns4KafkaProperties;

    @Get("/version")
    public AppInfo version() {
        return AppInfo.builder()
            .version(ns4KafkaProperties.getVersion())
            .build();
    }
}
