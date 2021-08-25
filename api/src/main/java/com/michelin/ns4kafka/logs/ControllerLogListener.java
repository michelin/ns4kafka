package com.michelin.ns4kafka.logs;

import com.michelin.ns4kafka.controllers.ResourceController;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.annotation.Async;
import io.micronaut.security.utils.SecurityService;
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
public class ControllerLogListener implements ApplicationEventListener<ResourceController.AuditLog> {

    TreeMap<Long, AuditLogControllerModel> inMemoryDatastore = new TreeMap<>();

    @Override
    @Async
    public void onApplicationEvent(ResourceController.AuditLog event) {

        inMemoryDatastore.put(event.getTimestamp(),
                new AuditLogControllerModel(
                        event.getUser().username(),
                        event.getDate(),
                        event.getTimestamp(),
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
        private Long timestamp;
        private String kind;
        private ObjectMeta metadata;
        private String operation;
        private String before;
        private String after;


    }
}
