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

package com.michelin.ns4kafka.log;

import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.model.AuditLog;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Console log listener.
 */
@Slf4j
@Singleton
@Requires(property = "ns4kafka.log.console.enabled", notEquals = StringUtils.FALSE)
public class ConsoleLogListener implements ApplicationEventListener<AuditLog> {

    @Override
    public void onApplicationEvent(AuditLog event) {
        log.info("{} {} {} {} {}{} in namespace {} on cluster {}.",
            event.isAdmin() ? "Admin" : "User",
            event.getUser(),
            event.getOperation(),
            event.getKind(),
            event.getMetadata().getName(),
            StringUtils.isEmpty(event.getVersion()) ? EMPTY_STRING : " version " + event.getVersion(),
            event.getMetadata().getNamespace(),
            event.getMetadata().getCluster()
        );
    }
}
