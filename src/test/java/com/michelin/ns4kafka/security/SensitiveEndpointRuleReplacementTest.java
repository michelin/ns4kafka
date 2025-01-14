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

import static com.michelin.ns4kafka.security.auth.JwtCustomClaimNames.ROLES;
import static com.nimbusds.jwt.JWTClaimNames.SUBJECT;
import static io.micronaut.security.rules.SecurityRuleResult.ALLOWED;
import static io.micronaut.security.rules.SecurityRuleResult.REJECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micronaut.http.HttpRequest;
import io.micronaut.management.endpoint.EndpointSensitivityProcessor;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRuleResult;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class SensitiveEndpointRuleReplacementTest {
    @Mock
    EndpointSensitivityProcessor endpointSensitivityProcessor;

    @InjectMocks
    SensitiveEndpointRuleReplacement sensitiveEndpointRuleReplacement;

    @Test
    void shouldRejectNonAdmin() {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of());
        Authentication auth = Authentication.build("user", List.of(), claims);

        Publisher<SecurityRuleResult> actual = sensitiveEndpointRuleReplacement
            .checkSensitiveAuthenticated(HttpRequest.GET("/anything"), auth, null);

        StepVerifier.create(actual)
            .consumeNextWith(response -> assertEquals(REJECTED, response))
            .verifyComplete();
    }

    @Test
    void shouldAllowAdmin() {
        Map<String, Object> claims = Map.of(SUBJECT, "user", ROLES, List.of("isAdmin()"));
        Authentication auth = Authentication.build("user", List.of("isAdmin()"), claims);

        Publisher<SecurityRuleResult> actual = sensitiveEndpointRuleReplacement
            .checkSensitiveAuthenticated(HttpRequest.GET("/anything"), auth, null);

        StepVerifier.create(actual)
            .consumeNextWith(response -> assertEquals(ALLOWED, response))
            .verifyComplete();
    }
}
