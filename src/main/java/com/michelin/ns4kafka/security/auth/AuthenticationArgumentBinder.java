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
package com.michelin.ns4kafka.security.auth;

import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.bind.binders.TypedRequestArgumentBinder;
import io.micronaut.security.authentication.Authentication;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * Authentication argument binder. Binds the AuthenticationInfo from the Authentication, so it can be injected in the
 * controllers.
 *
 * @see <a href="https://micronaut-projects.github.io/micronaut-security/latest/guide/#customAuthenticatedUser">
 *     Micronaut Custom Binding</a>
 */
@Singleton
public class AuthenticationArgumentBinder implements TypedRequestArgumentBinder<AuthenticationInfo> {
    @Override
    public Argument<AuthenticationInfo> argumentType() {
        return Argument.of(AuthenticationInfo.class);
    }

    @Override
    public BindingResult<AuthenticationInfo> bind(
            ArgumentConversionContext<AuthenticationInfo> context, HttpRequest<?> source) {
        final Optional<Authentication> authentication = source.getUserPrincipal(Authentication.class);
        return authentication.isPresent() ? (() -> authentication.map(AuthenticationInfo::of)) : Optional::empty;
    }
}
