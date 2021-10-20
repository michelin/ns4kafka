package com.michelin.ns4kafka.services.listeners;

import com.michelin.ns4kafka.services.listeners.events.AbstractResourceEvent;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.scheduling.annotation.Async;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

@Slf4j
@Singleton
public class ResourceEventListener implements ApplicationEventListener<AbstractResourceEvent<?>> {
    /**
     * The application context
     */
    @Inject
    public ApplicationContext applicationContext;

    /**
     * Process the received resource event
     *
     * @param event The resource event
     */
    @Async
    @Override
    public void onApplicationEvent(AbstractResourceEvent event) {
        log.debug("New resource event received {}", event.getClass().getSimpleName());

        event.injectServices(applicationContext);

        event.process();
    }
}
