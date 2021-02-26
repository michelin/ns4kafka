package com.michelin.ns4kafka.authentication.gitlab;

import io.micronaut.context.annotation.Value;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.DefaultAuthentication;
import io.micronaut.security.token.validator.TokenValidator;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;

@Singleton
public class GitlabTokenValidator implements TokenValidator {
    private static final Logger LOG = LoggerFactory.getLogger(GitlabTokenValidator.class);

    public static final String IS_ADMIN = "isAdmin()";
    @Inject
    GitlabAuthorizationService gitlabAuthorizationService;

    @Value("${ns4kafka.admin.group:_}")
    private String adminGroup;

    @Deprecated
    @Override
    public Publisher<Authentication> validateToken(String token) {
        return validateToken(token,null);
    }

    @Override
    public Publisher<Authentication> validateToken(String token, @Nullable HttpRequest<?> request) throws AuthenticationException{
        return StringUtils.isNotEmpty(token) ?
                gitlabAuthorizationService.findUsername(token)
                        .onErrorResumeNext(throwable -> {
                            LOG.error("Gitlab exception during Authentication step : "+ throwable.getMessage());
                            return Maybe.empty();
                        })
                        .flatMapPublisher(username -> gitlabAuthorizationService
                                .findAllGroups(token)
                                .toList()
                                .map(groups -> new DefaultAuthentication(username, Map.of(
                                        "groups", groups,
                                        "email", username,
                                        "roles", computeRolesFromProperties(groups)
                                )))
                                .toFlowable()
                        )
                : Flowable.empty();
    }

    private List<String> computeRolesFromProperties(List<String> groups) {
        if(groups.contains(adminGroup))
            return List.of(IS_ADMIN);
        //TODO other specific API groups ? auditor ?
        return List.of();
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
