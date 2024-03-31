@Configuration
@Requires(property = "ns4kafka.store.kafka.enabled", notEquals = StringUtils.FALSE)
package com.michelin.ns4kafka.repository.kafka;

import io.micronaut.context.annotation.Configuration;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;