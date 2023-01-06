package com.michelin.ns4kafka.security.local;

import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationProvider;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Singleton
public class LocalUserAuthenticationProvider implements AuthenticationProvider {
    @Inject
    SecurityConfig securityConfig;

    @Inject
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<?> httpRequest, AuthenticationRequest<?, ?> authenticationRequest) {
        return Flowable.create(emitter -> {
            String username = authenticationRequest.getIdentity().toString();
            String password = authenticationRequest.getSecret().toString();
            log.debug("Checking local authentication for user : {}", username);

            Optional<LocalUser> authenticatedUser = securityConfig.getLocalUsers().stream()
                    .filter(localUser -> localUser.getUsername().equals(username))
                    .filter(localUser -> localUser.isValidPassword(password))
                    .findFirst();
            if (authenticatedUser.isPresent()) {
                AuthenticationResponse user = AuthenticationResponse.success(username,
                        resourceBasedSecurityRule.computeRolesFromGroups(authenticatedUser.get().getGroups()),
                        Map.of("groups", authenticatedUser.get().getGroups()));
                emitter.onNext(user);
            }
            emitter.onComplete();

        }, BackpressureStrategy.ERROR);
    }
}
