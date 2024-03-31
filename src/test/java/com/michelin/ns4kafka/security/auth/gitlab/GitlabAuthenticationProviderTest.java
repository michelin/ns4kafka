package com.michelin.ns4kafka.security.auth.gitlab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.security.auth.AuthenticationRoleBinding;
import com.michelin.ns4kafka.security.auth.AuthenticationService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationRequest;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class GitlabAuthenticationProviderTest {
    @Mock
    AuthenticationService authenticationService;

    @Mock
    GitlabAuthenticationService gitlabAuthenticationService;

    @InjectMocks
    GitlabAuthenticationProvider gitlabAuthenticationProvider;

    @Test
    void authenticationSuccess() {
        AuthenticationRequest<String, String> authenticationRequest =
            new UsernamePasswordCredentials("username", "53cu23d_70k3n");

        List<String> groups = List.of("group-1", "group-2");

        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret()))
            .thenReturn(Mono.just("username"));
        when(gitlabAuthenticationService.findAllGroups(authenticationRequest.getSecret()))
            .thenReturn(Flux.fromIterable(groups));

        AuthenticationRoleBinding authenticationRoleBinding = AuthenticationRoleBinding.builder()
            .namespace("namespace")
            .verbs(List.of(RoleBinding.Verb.GET))
            .resourceTypes(List.of("topics"))
            .build();

        AuthenticationResponse authenticationResponse = AuthenticationResponse.success("username", null,
            Map.of("roleBindings", List.of(authenticationRoleBinding)));

        when(authenticationService.buildAuthJwtGroups("username", groups))
            .thenReturn(authenticationResponse);

        Publisher<AuthenticationResponse> authenticationResponsePublisher =
            gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        StepVerifier.create(authenticationResponsePublisher)
            .consumeNextWith(response -> {
                assertTrue(response.isAuthenticated());
                assertTrue(response.getAuthentication().isPresent());
                assertEquals("username", response.getAuthentication().get().getName());
                assertIterableEquals(List.of(authenticationRoleBinding),
                    (List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                        .get("roleBindings"));
                assertIterableEquals(List.of(), response.getAuthentication().get().getRoles(),
                    "User has no custom roles");
            })
            .verifyComplete();
    }

    @Test
    void authenticationSuccessAdmin() {
        AuthenticationRequest<String, String> authenticationRequest =
            new UsernamePasswordCredentials("usernameAdmin", "53cu23d_70k3n");

        List<String> groups = List.of("group-1", "group-2", "group-admin");

        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret()))
            .thenReturn(Mono.just("usernameAdmin"));
        when(gitlabAuthenticationService.findAllGroups(authenticationRequest.getSecret()))
            .thenReturn(Flux.fromIterable(groups));

        AuthenticationRoleBinding authenticationRoleBinding = AuthenticationRoleBinding.builder()
            .namespace("namespace")
            .verbs(List.of(RoleBinding.Verb.GET))
            .resourceTypes(List.of("topics"))
            .build();

        AuthenticationResponse authenticationResponse = AuthenticationResponse.success("usernameAdmin",
            List.of(ResourceBasedSecurityRule.IS_ADMIN), Map.of("roleBindings", List.of(authenticationRoleBinding)));

        when(authenticationService.buildAuthJwtGroups("usernameAdmin", groups))
            .thenReturn(authenticationResponse);

        Publisher<AuthenticationResponse> authenticationResponsePublisher =
            gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        StepVerifier.create(authenticationResponsePublisher)
            .consumeNextWith(response -> {
                assertTrue(response.isAuthenticated());
                assertTrue(response.getAuthentication().isPresent());
                assertEquals("usernameAdmin", response.getAuthentication().get().getName());
                assertIterableEquals(List.of(authenticationRoleBinding),
                    (List<AuthenticationRoleBinding>) response.getAuthentication().get().getAttributes()
                        .get("roleBindings"));
                assertIterableEquals(List.of(ResourceBasedSecurityRule.IS_ADMIN),
                    response.getAuthentication().get().getRoles(),
                    "User has no custom roles");
            })
            .verifyComplete();
    }

    @Test
    void authenticationFailureUsername() {
        AuthenticationRequest<String, String> authenticationRequest =
            new UsernamePasswordCredentials("username", "f4k3_70k3n");

        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret()))
            .thenReturn(Mono.error(new HttpClientResponseException("403 Unauthorized", HttpResponse.unauthorized())));

        Publisher<AuthenticationResponse> authenticationResponsePublisher =
            gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        StepVerifier.create(authenticationResponsePublisher)
            .consumeErrorWith(error -> assertEquals(AuthenticationException.class, error.getClass()))
            .verify();
    }

    @Test
    void authenticationFailureUsernameGroups() {
        AuthenticationRequest<String, String> authenticationRequest =
            new UsernamePasswordCredentials("username", "f4k3_70k3n");

        when(gitlabAuthenticationService.findUsername(authenticationRequest.getSecret()))
            .thenReturn(Mono.just("username"));
        when(gitlabAuthenticationService.findAllGroups(authenticationRequest.getSecret()))
            .thenReturn(Flux.error(new HttpClientResponseException("Error", HttpResponse.unauthorized())));

        Publisher<AuthenticationResponse> authenticationResponsePublisher =
            gitlabAuthenticationProvider.authenticate(null, authenticationRequest);

        StepVerifier.create(authenticationResponsePublisher)
            .consumeErrorWith(error -> assertEquals(AuthenticationException.class, error.getClass()))
            .verify();
    }
}
