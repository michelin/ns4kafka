package com.michelin.ns4kafka.authentication.gitlab;

import com.michelin.ns4kafka.security.RessourceBasedSecurityRule;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.*;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

//@Singleton not ready
public class GitlabAuthenticationProvider implements AuthenticationProvider {
    private static final Logger LOG = LoggerFactory.getLogger(GitlabAuthenticationProvider.class);
    @Inject GitlabAuthorizationService gitlabAuthorizationService;
    @Override
    public Publisher<AuthenticationResponse> authenticate(@Nullable HttpRequest<?> httpRequest, AuthenticationRequest<?,?> authenticationRequest) {
        UsernamePasswordCredentials credentials = (UsernamePasswordCredentials) authenticationRequest;
        String username = gitlabAuthorizationService.findUsername(credentials.getSecret()).blockingGet();
        List<String> groups = gitlabAuthorizationService.findAllGroups(credentials.getSecret()).toList().blockingGet();
        //String username = "toto";
        //List<String> groups = List.of("test","f4m");
        //eturn Flowable.create(new UserDetails("ssss", List.of()));
        UserDetails user = new UserDetails(username, List.of(RessourceBasedSecurityRule.IS_AUTHENTICATED), Map.of("groups",groups));
        return Publishers.just(user);
    }
}
