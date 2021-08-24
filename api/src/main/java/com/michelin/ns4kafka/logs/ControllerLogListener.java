package com.michelin.ns4kafka.logs;

import com.michelin.ns4kafka.controllers.ResourceController;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.context.event.ApplicationEventListener;
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
public class ControllerLogListener implements ApplicationEventListener<ResourceController.AuditLog> {

    TreeMap<Long, ResourceController.AuditLog> inMemoryDatastore = new TreeMap<>();

    @Override
    @Async
    public void onApplicationEvent(ResourceController.AuditLog event) {
        inMemoryDatastore.put(event.getTimestamp(), event);
    }

    @Get("/")
    public Collection<ResourceController.AuditLog> getLogsFromLastHour() {
        // Default way to get logs
        return getLogsFromDuration(Duration.ofHours(1));
    }

    @Get("/from-duration{?duration}")
    public Collection<ResourceController.AuditLog> getLogsFromDuration(@QueryValue(defaultValue = "1H") Duration duration){
        return inMemoryDatastore.tailMap(Instant.now().getEpochSecond() - duration.getSeconds()).values();
    }
}
