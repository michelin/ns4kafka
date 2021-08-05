package com.michelin.ns4kafka.logs;

import com.michelin.ns4kafka.controllers.ResourceController;
import io.micronaut.context.event.ApplicationEventListener;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

@Slf4j
@Singleton
public class ForgedLogListener implements ApplicationEventListener<ResourceController.AuditLog> {

    @Override
    public void onApplicationEvent(ResourceController.AuditLog event) {
        String role = "User";
        if (event.getUser().hasRole("isAdmin()")) {
            role = "Admin";
        }
        String user = null;
        if (event.getUser().username().isPresent()){
            user = event.getUser().username().get();
        }

        log.info("{} {} {} at {} {} {}",
                role,
                user,
                event.getOperation(),
                event.getDate().toString(),
                event.getKind(),
                event.getMetadata().toString());
    }
}
