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
package com.michelin.ns4kafka.security.auth.ldap;

import com.michelin.ns4kafka.security.auth.AuthenticationService;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.convert.value.ConvertibleValues;
import io.micronaut.core.util.StringUtils;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.ldap.ContextAuthenticationMapper;
import io.micronaut.security.ldap.DefaultContextAuthenticationMapper;
import io.micronaut.security.ldap.configuration.LdapConfiguration;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Set;

/** Custom LDAP authentication mapper. */
@Singleton
@Replaces(DefaultContextAuthenticationMapper.class)
@Requires(property = LdapConfiguration.PREFIX + ".enabled", notEquals = StringUtils.FALSE)
public class LdapAuthenticationMapper implements ContextAuthenticationMapper {
    @Inject
    AuthenticationService authenticationService;

    @Override
    public AuthenticationResponse map(ConvertibleValues<Object> attributes, String username, Set<String> groups) {
        return authenticationService.buildAuthJwtGroups(username, List.copyOf(groups));
    }
}
