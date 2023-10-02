package com.michelin.ns4kafka.controllers.generic;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.NamespaceService;
import jakarta.inject.Inject;

/**
 * Namespaced resource controller.
 */
public abstract class NamespacedResourceController extends ResourceController {
    @Inject
    private NamespaceService namespaceService;

    /**
     * Call this to get the Namespace associated with the current request.
     *
     * @param namespace the namespace String
     * @return the Namespace associated with the current request.
     */
    public Namespace getNamespace(String namespace) {
        return namespaceService.findByName(namespace).orElseThrow();
    }
}
