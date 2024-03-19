package com.michelin.ns4kafka.security.auth.local;

import com.michelin.ns4kafka.properties.SecurityProperties;
import com.michelin.ns4kafka.security.auth.AuthenticationService;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationFailureReason;
import io.micronaut.security.authentication.AuthenticationProvider;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * Local user authentication provider.
 */
@Slf4j
@Singleton
public class LocalUserAuthenticationProvider implements AuthenticationProvider<HttpRequest<?>> {
    @Inject
    SecurityProperties securityProperties;

    @Inject
    AuthenticationService authenticationService;

    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<?> httpRequest,
                                                          AuthenticationRequest<?, ?> authenticationRequest) {
        return Mono.create(emitter -> {
            String username = authenticationRequest.getIdentity().toString();
            String password = authenticationRequest.getSecret().toString();
            log.debug("Checking local authentication for user: {}", username);

            Optional<LocalUser> authenticatedUser = securityProperties.getLocalUsers().stream()
                .filter(localUser -> localUser.getUsername().equals(username))
                .filter(localUser -> localUser.isValidPassword(password))
                .findFirst();

            if (authenticatedUser.isPresent()) {
                emitter.success(
                    authenticationService.buildAuthJwtGroups(username, authenticatedUser.get().getGroups()));
            } else {
                emitter.error(new AuthenticationException(
                    new AuthenticationFailed(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH)));
            }
        });
    }
}
