package com.michelin.ns4kafka.authentication;

import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
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

@Singleton
public class GitlabTokenValidator implements TokenValidator {
    private static final Logger LOG = LoggerFactory.getLogger(GitlabTokenValidator.class);

    public GitlabTokenValidator(){}
    @Override
    public Publisher<Authentication> validateToken(String token) {
        return validateToken(token,null);
    }

    @Override
    public Publisher<Authentication> validateToken(String token, @Nullable HttpRequest<?> request) {
        //Perform Authent
        if (token.equals("123")) {
            return  Flowable.just(new DefaultAuthentication(token, Map.of("roles", List.of("Admin"))));
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.info("Could not authenticate {}", token);
            }
            return Flowable.empty();
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
