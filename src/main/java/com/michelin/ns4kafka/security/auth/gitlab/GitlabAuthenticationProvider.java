package com.michelin.ns4kafka.security.auth.gitlab;

import com.michelin.ns4kafka.security.auth.AuthenticationService;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationProvider;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * Gitlab authentication provider.
 */
@Slf4j
@Singleton
public class GitlabAuthenticationProvider implements AuthenticationProvider<HttpRequest<?>> {
    @Inject
    GitlabAuthenticationService gitlabAuthenticationService;

    @Inject
    AuthenticationService authenticationService;

    /**
     * Perform user authentication with GitLab.
     *
     * @param httpRequest           The HTTP request
     * @param authenticationRequest The authentication request
     * @return An authentication response with the user details
     */
    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<?> httpRequest,
                                                          AuthenticationRequest<?, ?> authenticationRequest) {
        String token = authenticationRequest.getSecret().toString();

        log.debug("Checking authentication with token {}", token);

        return gitlabAuthenticationService.findUsername(token)
            .onErrorResume(
                error -> Mono.error(new AuthenticationException(new AuthenticationFailed("Bad GitLab token"))))
            .flatMap(username -> gitlabAuthenticationService.findAllGroups(token).collectList()
                .onErrorResume(error -> Mono.error(
                    new AuthenticationException(new AuthenticationFailed("Cannot retrieve your GitLab groups"))))
                .flatMap(groups -> Mono.just(authenticationService.buildAuthJwtGroups(username, groups))));
    }

}
