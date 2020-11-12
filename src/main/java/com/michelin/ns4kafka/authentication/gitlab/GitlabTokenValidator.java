package com.michelin.ns4kafka.authentication.gitlab;

import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.DefaultAuthentication;
import io.micronaut.security.token.validator.TokenValidator;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Singleton
public class GitlabTokenValidator implements TokenValidator {
    private static final Logger LOG = LoggerFactory.getLogger(GitlabTokenValidator.class);

    @Inject
    GitlabAuthorizationService gitlabAuthorizationService;

    @Deprecated
    @Override
    public Publisher<Authentication> validateToken(String token) {
        return validateToken(token,null);
    }

    @Override
    public Publisher<Authentication> validateToken(String token, @Nullable HttpRequest<?> request) throws AuthenticationException{
        if(StringUtils.isNotEmpty(token)) {
            try{
                // This will throw 403 if token is not valid
                List<Map<String, Object>> list =  gitlabAuthorizationService.findGroups(token).blockingGet();
                if (list.size() > 0) {
                    List<String> groups = list.stream()
                            .map(item -> item.get("full_path").toString())
                            .collect(Collectors.toList());
                    return Flowable.just(new DefaultAuthentication(token, Map.of("roles", groups)));

                } else {
                    return Flowable.empty();
                }
            } catch (HttpClientResponseException resp) {
                LOG.debug("Received Invalid response from gitlab API",resp);
                return Flowable.empty();
            }
        }else{
            return Flowable.empty();
        }

    }

    @Override
    public int getOrder() {
        return 0;
    }
}
