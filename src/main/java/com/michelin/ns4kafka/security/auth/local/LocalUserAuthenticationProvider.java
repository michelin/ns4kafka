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
package com.michelin.ns4kafka.security.auth.local;

import com.michelin.ns4kafka.property.SecurityProperties;
import com.michelin.ns4kafka.security.auth.AuthenticationService;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationFailureReason;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.provider.ReactiveAuthenticationProvider;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/** Local user authentication provider. */
@Slf4j
@Singleton
public class LocalUserAuthenticationProvider implements ReactiveAuthenticationProvider<HttpRequest<?>, String, String> {
    @Inject
    SecurityProperties securityProperties;

    @Inject
    AuthenticationService authenticationService;

    @Override
    public @NonNull Publisher<AuthenticationResponse> authenticate(
            @Nullable HttpRequest<?> requestContext,
            @NonNull AuthenticationRequest<String, String> authenticationRequest) {
        return Mono.create(emitter -> {
            String username = authenticationRequest.getIdentity();
            String password = authenticationRequest.getSecret();
            log.debug("Checking local authentication for user: {}", username);

            Optional<LocalUser> authenticatedUser = securityProperties.getLocalUsers().stream()
                    .filter(localUser -> localUser.getUsername().equals(username))
                    .filter(localUser -> localUser.isValidPassword(password))
                    .findFirst();

            if (authenticatedUser.isPresent()) {
                emitter.success(authenticationService.buildAuthJwtGroups(
                        username, authenticatedUser.get().getGroups()));
            } else {
                emitter.error(new AuthenticationException(
                        new AuthenticationFailed(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH)));
            }
        });
    }
}
