package com.michelin.ns4kafka.security.auth;

import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.bind.binders.TypedRequestArgumentBinder;
import io.micronaut.security.authentication.Authentication;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * Authentication argument binder.
 * Binds the AuthenticationInfo from the Authentication, so it can be injected in the controllers.
 *
 * @see <a href="https://micronaut-projects.github.io/micronaut-security/latest/guide/#customAuthenticatedUser">Micronaut Custom Binding</a>
 */
@Singleton
public class AuthenticationArgumentBinder implements TypedRequestArgumentBinder<AuthenticationInfo> {
    @Override
    public Argument<AuthenticationInfo> argumentType() {
        return Argument.of(AuthenticationInfo.class);
    }

    @Override
    public BindingResult<AuthenticationInfo> bind(ArgumentConversionContext<AuthenticationInfo> context,
                                                  HttpRequest<?> source) {
        final Optional<Authentication> authentication = source.getUserPrincipal(Authentication.class);
        return authentication.isPresent() ? (() -> authentication.map(AuthenticationInfo::of))
            : Optional::empty;
    }
}
