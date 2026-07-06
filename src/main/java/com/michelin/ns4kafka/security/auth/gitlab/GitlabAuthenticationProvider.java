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
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.provider.ReactiveAuthenticationProvider;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/** Gitlab authentication provider. */
@Slf4j
@Singleton
public class GitlabAuthenticationProvider implements ReactiveAuthenticationProvider<HttpRequest<?>, String, String> {
    private final GitlabAuthenticationService gitlabAuthenticationService;
    private final AuthenticationService authenticationService;

    /**
     * Constructor.
     *
     * @param gitlabAuthenticationService The Gitlab authentication service
     * @param authenticationService The authentication service
     */
    public GitlabAuthenticationProvider(
            GitlabAuthenticationService gitlabAuthenticationService, AuthenticationService authenticationService) {
        this.gitlabAuthenticationService = gitlabAuthenticationService;
        this.authenticationService = authenticationService;
    }

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
                .onErrorResume(throwable ->
                        Mono.error(new AuthenticationException(new AuthenticationFailed(throwable.getMessage()))))
                .flatMap(username -> buildAuthResponse(username, token));
    }

    /**
     * Build the authentication response for the given user.
     *
     * @param username The username
     * @param token The user token
     * @return The authentication response
     */
    private Mono<AuthenticationResponse> buildAuthResponse(String username, String token) {
        return gitlabAuthenticationService
                .findGuestGroups(token)
                .collectList()
                .onErrorResume(_ -> Mono.error(new AuthenticationException(
                        new AuthenticationFailed("Cannot retrieve your GitLab groups from GitLab"))))
                .flatMap(groups -> Mono.fromCallable(() -> authenticationService.buildAuthJwtGroups(username, groups))
                        .onErrorResume(
                                AuthenticationException.class,
                                _ -> gitlabAuthenticationService
                                        .findAllAvailableGroups(token)
                                        .collectList()
                                        .onErrorResume(
                                                _ -> Mono.error(new AuthenticationException(new AuthenticationFailed(
                                                        "Cannot retrieve the GitLab groups you were invited to"))))
                                        .map(fallbackGroups ->
                                                authenticationService.buildAuthJwtGroups(username, fallbackGroups))));
    }
}
