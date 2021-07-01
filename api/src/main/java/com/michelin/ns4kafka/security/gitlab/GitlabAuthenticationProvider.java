package com.michelin.ns4kafka.security.gitlab;

import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.*;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;

@Slf4j
@Singleton
public class GitlabAuthenticationProvider implements AuthenticationProvider {

    @Inject
    GitlabAuthenticationService gitlabAuthenticationService;

    @Inject
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<?> httpRequest, AuthenticationRequest<?,?> authenticationRequest) {
        Flowable<AuthenticationResponse> responseFlowable = Flowable.create(emitter -> {
            String token = authenticationRequest.getSecret().toString();
            log.debug("Checking authentication with token : {}",token);
            try {
                String username = gitlabAuthenticationService.findUsername(token).blockingGet();
                List<String> groups = gitlabAuthenticationService.findAllGroups(token).toList().blockingGet();

                UserDetails user = new UserDetails(username, resourceBasedSecurityRule.computeRolesFromGroups(groups), Map.of("groups", groups));

                emitter.onNext(user);
                emitter.onComplete();
            }catch (Exception e){
                log.debug("Exception during authenticate : {}",e.getMessage());
                emitter.onError(new AuthenticationException(new AuthenticationFailed(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH)));
            }

        }, BackpressureStrategy.ERROR);
        responseFlowable = responseFlowable.subscribeOn(Schedulers.io());
        return responseFlowable;
    }

}
