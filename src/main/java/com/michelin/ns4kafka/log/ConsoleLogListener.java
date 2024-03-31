package com.michelin.ns4kafka.log;

import com.michelin.ns4kafka.model.AuditLog;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Console log listener.
 */
@Slf4j
@Singleton
@Requires(property = "ns4kafka.log.console.enabled", notEquals = StringUtils.FALSE)
public class ConsoleLogListener implements ApplicationEventListener<AuditLog> {

    @Override
    public void onApplicationEvent(AuditLog event) {
        log.info("{} {} {} {} {} in namespace {} on cluster {}.",
            event.isAdmin() ? "Admin" : "User",
            event.getUser(),
            event.getOperation(),
            event.getKind(),
            event.getMetadata().getName(),
            event.getMetadata().getNamespace(),
            event.getMetadata().getCluster()
        );
    }
}
