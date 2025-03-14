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
package com.michelin.ns4kafka.security;

import static com.michelin.ns4kafka.security.ResourceBasedSecurityRule.IS_ADMIN;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.http.HttpRequest;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.management.endpoint.EndpointSensitivityProcessor;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.security.rules.SensitiveEndpointRule;
import jakarta.inject.Singleton;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/** Security rule to check if a user can access a given sensitive endpoint. */
@Slf4j
@Singleton
@Replaces(SensitiveEndpointRule.class)
public class SensitiveEndpointRuleReplacement extends SensitiveEndpointRule {
    /**
     * Constructor.
     *
     * @param endpointSensitivityProcessor The endpoint configurations
     */
    public SensitiveEndpointRuleReplacement(EndpointSensitivityProcessor endpointSensitivityProcessor) {
        super(endpointSensitivityProcessor);
    }

    @Override
    protected @NonNull Publisher<SecurityRuleResult> checkSensitiveAuthenticated(
            @NonNull HttpRequest<?> request,
            @NonNull Authentication authentication,
            @NonNull ExecutableMethod<?, ?> method) {
        String sub = authentication.getName();
        Collection<String> roles = authentication.getRoles();
        if (roles.contains(IS_ADMIN)) {
            log.debug("Authorized admin \"{}\" on sensitive endpoint \"{}\"", sub, request.getPath());
            return Mono.just(SecurityRuleResult.ALLOWED);
        }

        return Mono.just(SecurityRuleResult.REJECTED);
    }
}
