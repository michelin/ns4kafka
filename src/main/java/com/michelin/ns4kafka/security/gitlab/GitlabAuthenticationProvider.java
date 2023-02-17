package com.michelin.ns4kafka.security.gitlab;

import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.RoleBindingService;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.*;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;


@Slf4j
@Singleton
public class GitlabAuthenticationProvider implements AuthenticationProvider {
    @Inject
    GitlabAuthenticationService gitlabAuthenticationService;

    @Inject
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Inject
    RoleBindingService roleBindingService;

    @Inject
    SecurityConfig securityConfig;

    /**
     * Perform user authentication with GitLab
     * @param httpRequest The HTTP request
     * @param authenticationRequest The authentication request
     * @return An authentication response with the user details
     */
    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<?> httpRequest, AuthenticationRequest<?,?> authenticationRequest) {
        Flowable<AuthenticationResponse> responseFlowable = Flowable.create(emitter -> {
            String token = authenticationRequest.getSecret().toString();

            log.debug("Checking authentication with token: {}", token);

            try {
                String username = gitlabAuthenticationService.findUsername(token).blockingGet();
                List<String> groups = gitlabAuthenticationService.findAllGroups(token).toList().blockingGet();

                if (roleBindingService.listByGroups(groups).isEmpty() && !groups.contains(securityConfig.getAdminGroup())) {
                    log.debug("Error during authentication: user groups not found in any namespace");
                    emitter.onError(new AuthenticationException(new AuthenticationFailed("User groups not found in any namespace. There may be an error on the GitLab group of your namespace.")));
                } else {
                    AuthenticationResponse user = AuthenticationResponse.success(username, resourceBasedSecurityRule.computeRolesFromGroups(groups), Map.of("groups", groups));
                    emitter.onNext(user);
                    emitter.onComplete();
                }
            } catch (Exception e) {
                log.debug("Exception during authentication: {}", e.getMessage());
                emitter.onError(new AuthenticationException(new AuthenticationFailed(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH)));
            }

        }, BackpressureStrategy.ERROR);

        return responseFlowable.subscribeOn(Schedulers.io());
    }

}
