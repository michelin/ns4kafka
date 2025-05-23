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
package com.michelin.ns4kafka.security.auth.gitlab;

import com.michelin.ns4kafka.security.auth.AuthenticationService;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.provider.ReactiveAuthenticationProvider;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/** Gitlab authentication provider. */
@Slf4j
@Singleton
public class GitlabAuthenticationProvider implements ReactiveAuthenticationProvider<HttpRequest<?>, String, String> {
    @Inject
    GitlabAuthenticationService gitlabAuthenticationService;

    @Inject
    AuthenticationService authenticationService;

    /**
     * Perform user authentication with GitLab.
     *
     * @param requestContext The HTTP request
     * @param authenticationRequest The authentication request
     * @return An authentication response with the user details
     */
    @Override
    public @NonNull Publisher<AuthenticationResponse> authenticate(
            @Nullable HttpRequest<?> requestContext,
            @NonNull AuthenticationRequest<String, String> authenticationRequest) {
        String token = authenticationRequest.getSecret();

        log.debug("Checking authentication with token {}", token);

        return gitlabAuthenticationService
                .findUsername(token)
                .onErrorResume(error -> {
                    if (error instanceof HttpClientResponseException) {
                        String correlationId = ((HttpClientResponseException) error)
                                .getResponse()
                                .getHeaders()
                                .get("x-gitlab-meta");
                        log.error(
                                "An error occurred when retrieving the user info with their gitlab token {}",
                                correlationId);
                    }
                    log.error(error.getMessage(), error);

                    return (Mono.error(
                            new AuthenticationException(new AuthenticationFailed(String.format(error.getMessage())))));
                })
                .flatMap(username -> gitlabAuthenticationService
                        .findAllGroups(token)
                        .collectList()
                        .onErrorResume(error -> {
                            if (error instanceof HttpClientResponseException) {
                                String correlationId = ((HttpClientResponseException) error)
                                        .getResponse()
                                        .getHeaders()
                                        .get("x-gitlab-meta");
                                log.error(
                                        "An error occurred when retrieving the user groups with their gitlab token {}",
                                        correlationId);
                            }
                            log.error(error.getMessage(), error);

                            return Mono.error(new AuthenticationException(new AuthenticationFailed(
                                    String.format("Cannot retrieve your GitLab groups: %s", error.getMessage()))));
                        })
                        .flatMap(groups -> Mono.just(authenticationService.buildAuthJwtGroups(username, groups))));
    }
}
