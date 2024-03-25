@Configuration
@Requires(property = "micronaut.security.gitlab.enabled", notEquals = StringUtils.FALSE)
package com.michelin.ns4kafka.security.auth.gitlab;

import io.micronaut.context.annotation.Configuration;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;

