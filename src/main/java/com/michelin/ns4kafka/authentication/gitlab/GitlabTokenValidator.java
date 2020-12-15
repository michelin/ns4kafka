package com.michelin.ns4kafka.authentication.gitlab;

import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.DefaultAuthentication;
import io.micronaut.security.token.validator.TokenValidator;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
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
            return gitlabAuthorizationService.findUsername(token)
                    .onErrorResumeNext(throwable -> {
                        LOG.debug("Gitlab exception during Authentication step", throwable);
                        return Maybe.empty();
                    })
                    .flatMapPublisher(username -> {
                        return gitlabAuthorizationService.findGroups(token)
                                .onErrorResumeNext(throwable -> {
                                    LOG.debug("Gitlab exception during Authentication step", throwable);
                                    return Maybe.empty();
                                })
                                .flatMapPublisher(response -> {
                                    List<String> groups = response.stream()
                                            .map(item -> item.get("full_path").toString())
                                            .collect(Collectors.toList());
                                    return Flowable.just(new DefaultAuthentication(username, Map.of("roles", groups)));
                                });
                    });

        }else{
            return Flowable.empty();
        }

    }

    @Override
    public int getOrder() {
        return 0;
    }
}
