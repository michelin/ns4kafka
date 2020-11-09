package com.michelin.ns4kafka.authentication;

import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import io.micronaut.security.token.reader.HttpHeaderTokenReader;

import javax.inject.Singleton;

@Requires(property = "micronaut.security.gitlab.enabled", notEquals = StringUtils.FALSE)
@Singleton
public class GitlabHeaderTokenReader extends HttpHeaderTokenReader {
    public GitlabHeaderTokenReader(){}
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
