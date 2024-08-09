package com.michelin.ns4kafka.security.auth.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.property.SecurityProperties;
import com.michelin.ns4kafka.security.auth.AuthenticationRoleBinding;
import com.michelin.ns4kafka.security.auth.AuthenticationService;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
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
class LocalUserAuthenticationProviderTest {
    @Mock
    AuthenticationService authenticationService;

    @Mock
    SecurityProperties securityProperties;

    @InjectMocks
    LocalUserAuthenticationProvider localUserAuthenticationProvider;

    @Test
    void authenticateNoMatchUser() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        when(securityProperties.getLocalUsers())
            .thenReturn(List.of());

        Publisher<AuthenticationResponse> authenticationResponsePublisher =
            localUserAuthenticationProvider.authenticate(null, credentials);

        StepVerifier.create(authenticationResponsePublisher)
            .consumeErrorWith(error -> assertEquals(AuthenticationException.class, error.getClass()))
            .verify();
    }

    @Test
    void authenticateMatchUserNoMatchPassword() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        when(securityProperties.getLocalUsers())
            .thenReturn(List.of(LocalUser.builder()
                .username("admin")
                .password("invalid_sha256_signature")
                .build()));

        Publisher<AuthenticationResponse> authenticationResponsePublisher =
            localUserAuthenticationProvider.authenticate(null,
                credentials);

        StepVerifier.create(authenticationResponsePublisher)
            .consumeErrorWith(error -> assertEquals(AuthenticationException.class, error.getClass()))
            .verify();
    }

    @Test
    @SuppressWarnings("unchecked")
    void authenticateMatchUserMatchPassword() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");

        when(securityProperties.getLocalUsers())
            .thenReturn(List.of(LocalUser.builder()
                .username("admin")
                .password("8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918")
                .groups(List.of("admin"))
                .build()));

        AuthenticationRoleBinding authenticationRoleBinding = AuthenticationRoleBinding.builder()
            .namespace("namespace")
            .verbs(List.of(RoleBinding.Verb.GET))
            .resourceTypes(List.of("topics"))
            .build();

        AuthenticationResponse authenticationResponse = AuthenticationResponse.success("admin", null,
            Map.of("roleBindings", List.of(authenticationRoleBinding)));

        when(authenticationService.buildAuthJwtGroups("admin", List.of("admin")))
            .thenReturn(authenticationResponse);

        Publisher<AuthenticationResponse> authenticationResponsePublisher =
            localUserAuthenticationProvider.authenticate(null, credentials);

        StepVerifier.create(authenticationResponsePublisher)
            .consumeNextWith(response -> {
                assertTrue(response.isAuthenticated());
                assertTrue(response.getAuthentication().isPresent());
                assertEquals("admin", response.getAuthentication().get().getName());
                assertIterableEquals(List.of(authenticationRoleBinding),
                    (List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                        .get("roleBindings"));
            })
            .verifyComplete();
    }
}
