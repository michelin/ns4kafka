package com.michelin.ns4kafka.security.local;

import com.michelin.ns4kafka.properties.SecurityProperties;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.*;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Singleton
public class LocalUserAuthenticationProvider implements AuthenticationProvider<HttpRequest<?>> {
    @Inject
    SecurityProperties securityProperties;

    @Inject
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<?> httpRequest, AuthenticationRequest<?, ?> authenticationRequest) {
        return Mono.create(emitter -> {
            String username = authenticationRequest.getIdentity().toString();
            String password = authenticationRequest.getSecret().toString();
            log.debug("Checking local authentication for user: {}", username);

            Optional<LocalUser> authenticatedUser = securityProperties.getLocalUsers().stream()
                    .filter(localUser -> localUser.getUsername().equals(username))
                    .filter(localUser -> localUser.isValidPassword(password))
                    .findFirst();

            if (authenticatedUser.isPresent()) {
                AuthenticationResponse user = AuthenticationResponse.success(username,
                        resourceBasedSecurityRule.computeRolesFromGroups(authenticatedUser.get().getGroups()),
                        Map.of("groups", authenticatedUser.get().getGroups()));
                emitter.success(user);
            } else {
                emitter.error(new AuthenticationException(new AuthenticationFailed(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH)));
            }
        });
    }
}
