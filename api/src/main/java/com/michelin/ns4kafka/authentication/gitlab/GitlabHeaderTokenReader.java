package com.michelin.ns4kafka.authentication.gitlab;

import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import io.micronaut.security.token.reader.HttpHeaderTokenReader;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeIn;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityScheme;

import javax.inject.Singleton;

@Singleton
public class GitlabHeaderTokenReader extends HttpHeaderTokenReader {

    @Override
    protected String getPrefix() {
        return null;
    }

    @Override
    protected String getHeaderName() {
        return "X-Gitlab-Token";
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
