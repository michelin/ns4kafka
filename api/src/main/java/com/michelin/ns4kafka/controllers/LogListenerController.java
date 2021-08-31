package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.annotation.Async;

import javax.annotation.security.RolesAllowed;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.TreeMap;

@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Controller("audit-logs")
@Requires(property = "ns4kafka.log.controller.enabled", notEquals = StringUtils.FALSE)
public class LogListenerController implements ApplicationEventListener<AuditLog> {

    TreeMap<Long, AuditLog> inMemoryDatastore = new TreeMap<>();

    @Override
    @Async
    public void onApplicationEvent(AuditLog event) {

        inMemoryDatastore.put(Instant.now().toEpochMilli(), event);
    }

    @Get("/")
    public Collection<AuditLog> getLogsFromLastHour() {
        // Default way to get logs
        return getLogsFromDuration(Duration.ofHours(1));
    }

    @Get("/from-duration{?duration}")
    public Collection<AuditLog> getLogsFromDuration(@QueryValue(defaultValue = "1H") Duration duration){
        return inMemoryDatastore.tailMap(Instant.now().toEpochMilli() - duration.toMillis()).values();
    }
}
