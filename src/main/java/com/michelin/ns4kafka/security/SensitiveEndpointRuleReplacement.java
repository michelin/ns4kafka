package com.michelin.ns4kafka.security;

import static com.michelin.ns4kafka.security.ResourceBasedSecurityRule.IS_ADMIN;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.http.HttpRequest;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.management.endpoint.EndpointSensitivityProcessor;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.security.rules.SensitiveEndpointRule;
import jakarta.inject.Singleton;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * Security rule to check if a user can access a given sensitive endpoint.
 */
@Slf4j
@Singleton
@Replaces(SensitiveEndpointRule.class)
public class SensitiveEndpointRuleReplacement extends SensitiveEndpointRule {
    /**
     * Constructor.
     *
     * @param endpointSensitivityProcessor The endpoint configurations
     */
    public SensitiveEndpointRuleReplacement(EndpointSensitivityProcessor endpointSensitivityProcessor) {
        super(endpointSensitivityProcessor);
    }

    @Override
    protected @NonNull Publisher<SecurityRuleResult> checkSensitiveAuthenticated(@NonNull HttpRequest<?> request,
                                                                                 @NonNull Authentication authentication,
                                                                                 @NonNull ExecutableMethod<?, ?> method
    ) {
        String sub = authentication.getName();
        Collection<String> roles = authentication.getRoles();
        if (roles.contains(IS_ADMIN)) {
            log.debug("Authorized admin \"{}\" on sensitive endpoint \"{}\"", sub, request.getPath());
            return Mono.just(SecurityRuleResult.ALLOWED);
        }

        return Mono.just(SecurityRuleResult.REJECTED);
    }
}
