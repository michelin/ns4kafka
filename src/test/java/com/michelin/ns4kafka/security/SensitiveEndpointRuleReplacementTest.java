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
