package com.michelin.ns4kafka.logs;

import com.michelin.ns4kafka.controllers.ApplyStatus;
import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.annotation.Async;
import lombok.Builder;
import lombok.Data;

import javax.annotation.security.RolesAllowed;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.TreeMap;

@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Controller("audit-logs")
public class ControllerLogListener implements ApplicationEventListener<AuditLog> {

    TreeMap<Long, AuditLogControllerModel> inMemoryDatastore = new TreeMap<>();

    @Override
    @Async
    public void onApplicationEvent(AuditLog event) {

        inMemoryDatastore.put(Instant.now().toEpochMilli(),
                new AuditLogControllerModel(
                        event.getUser().username(),
                        event.getDate(),
                        event.getKind(),
                        event.getMetadata(),
                        event.getOperation(),
                        event.getBefore(),
                        event.getAfter()));
    }

    @Get("/")
    public Collection<AuditLogControllerModel> getLogsFromLastHour() {
        // Default way to get logs
        return getLogsFromDuration(Duration.ofHours(1));
    }

    @Get("/from-duration{?duration}")
    public Collection<AuditLogControllerModel> getLogsFromDuration(@QueryValue(defaultValue = "1H") Duration duration){
        return inMemoryDatastore.tailMap(Instant.now().toEpochMilli() - duration.toMillis()).values();
    }

    @Builder
    @Data
    public static class AuditLogControllerModel {

        private Optional<String> user;
        private String date;
        private String kind;
        private ObjectMeta metadata;
        private ApplyStatus operation;
        private Object before;
        private Object after;


    }
}
