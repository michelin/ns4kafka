package com.michelin.ns4kafka.logs;

import com.michelin.ns4kafka.models.AuditLog;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.core.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

@Slf4j
@Singleton
@Requires(property = "ns4kafka.log.console.enabled", notEquals = StringUtils.FALSE)
public class ForgedLogListener implements ApplicationEventListener<AuditLog> {

    @Override
    public void onApplicationEvent(AuditLog event) {
        String role = event.isAdmin() ? "Admin" : "User";
        String user = event.getUser().isPresent() ? event.getUser().get() : null;

        log.info("{} {} {} at {} {} {}",
                role,
                user,
                event.getOperation(),
                event.getDate().toString(),
                event.getKind(),
                event.getMetadata().toString());
    }
}
