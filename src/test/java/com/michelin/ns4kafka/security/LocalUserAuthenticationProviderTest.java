package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.security.local.LocalUser;
import com.michelin.ns4kafka.security.local.LocalUserAuthenticationProvider;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalUserAuthenticationProviderTest {
    @Mock
    SecurityConfig securityConfig;

    @Mock
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @InjectMocks
    LocalUserAuthenticationProvider localUserAuthenticationProvider;

    @Test
    void authenticateNoMatchUser() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        when(securityConfig.getLocalUsers())
                .thenReturn(List.of());

        Publisher<AuthenticationResponse> authenticationResponsePublisher = localUserAuthenticationProvider.authenticate(null, credentials);

        StepVerifier.create(authenticationResponsePublisher)
                .consumeErrorWith(error -> assertEquals(AuthenticationException.class, error.getClass()))
                .verify();
    }

    @Test
    void authenticateMatchUserNoMatchPassword() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        when(securityConfig.getLocalUsers())
                .thenReturn(List.of(LocalUser.builder()
                        .username("admin")
                        .password("invalid_sha256_signature")
                        .build()));

        Publisher<AuthenticationResponse> authenticationResponsePublisher = localUserAuthenticationProvider.authenticate(null,
                credentials);

        StepVerifier.create(authenticationResponsePublisher)
                .consumeErrorWith(error -> assertEquals(AuthenticationException.class, error.getClass()))
                .verify();
    }

    @Test
    void authenticateMatchUserMatchPassword() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        when(securityConfig.getLocalUsers())
                .thenReturn(List.of(LocalUser.builder()
                        .username("admin")
                        .password("8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918")
                        .groups(List.of("admin"))
                        .build()));

        when(resourceBasedSecurityRule.computeRolesFromGroups(ArgumentMatchers.any()))
                .thenReturn(List.of());

        Publisher<AuthenticationResponse> authenticationResponsePublisher = localUserAuthenticationProvider.authenticate(null, credentials);

        StepVerifier.create(authenticationResponsePublisher)
                .consumeNextWith(response -> {
                    assertTrue(response.isAuthenticated());
                    assertTrue(response.getAuthentication().isPresent());
                    assertEquals("admin", response.getAuthentication().get().getName());
                })
                .verifyComplete();
    }
}
