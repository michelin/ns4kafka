package com.michelin.ns4kafka.services.listeners.events;

import io.micronaut.context.ApplicationContext;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class AbstractResourceEvent<T> {
    /**
     * The resource held by the event
     */
    public T resource;

    /**
     * Inject the services required to process the event
     */
    public abstract void injectServices(ApplicationContext applicationContext);

    /**
     * How to process this event
     */
    public abstract void process();
}
