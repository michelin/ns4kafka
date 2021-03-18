package com.michelin.ns4kafka.security.gitlab;

import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.*;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;


@Singleton
public class GitlabAuthenticationProvider implements AuthenticationProvider {
    private static final Logger LOG = LoggerFactory.getLogger(GitlabAuthenticationProvider.class);

    @Inject
    GitlabAuthenticationService gitlabAuthenticationService;

    @Inject
    ResourceBasedSecurityRule resourceBasedSecurityRule;

    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<?> httpRequest, AuthenticationRequest<?,?> authenticationRequest) {
        Flowable<AuthenticationResponse> responseFlowable = Flowable.create(emitter -> {
            String token = authenticationRequest.getSecret().toString();
            LOG.debug("Checking authentication with token : "+token);
            try {
                String username = gitlabAuthenticationService.findUsername(token).blockingGet();
                List<String> groups = gitlabAuthenticationService.findAllGroups(token).toList().blockingGet();

                UserDetails user = new UserDetails(username, resourceBasedSecurityRule.computeRolesFromGroups(groups), Map.of("groups", groups));

                emitter.onNext(user);
                emitter.onComplete();
            }catch (Exception e){
                LOG.debug("Exception during authenticate : "+e.getMessage());
                emitter.onError(new AuthenticationException(new AuthenticationFailed(AuthenticationFailureReason.CREDENTIALS_DO_NOT_MATCH)));
            }

        }, BackpressureStrategy.ERROR);
        responseFlowable = responseFlowable.subscribeOn(Schedulers.io());
        return responseFlowable;
    }

}
