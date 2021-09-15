package com.michelin.ns4kafka.logs;

import com.michelin.ns4kafka.models.AuditLog;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.core.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import jakarta.inject.Singleton;

@Slf4j
@Singleton
@Requires(property = "ns4kafka.log.console.enabled", notEquals = StringUtils.FALSE)
public class ConsoleLogListener implements ApplicationEventListener<AuditLog> {

    @Override
    public void onApplicationEvent(AuditLog event) {
        log.info("{} {} {} {} {} on cluster {}",
                event.isAdmin() ? "Admin" : "User",
                event.getUser(),
                event.getOperation(),
                event.getKind(),
                event.getMetadata().getName(),
                event.getMetadata().getCluster()
        );
    }
}
