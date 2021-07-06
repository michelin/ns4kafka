package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.NamespaceService;

import javax.inject.Inject;

/**
 * Base Controller for all Namespaced resources
 */
public abstract class NamespacedResourceController extends RessourceController {
    @Inject
    private NamespaceService namespaceService;

    /**
     * Call this to get the Namespace associated with the current request.
     * @param namespace the namespace String
     * @return the Namespace associated with the current request.
     * @exception java.util.NoSuchElementException if the namespace does not exist
     */
    public Namespace getNamespace(String namespace){
        return namespaceService.findByName(namespace).orElseThrow();
    }
}
