package com.michelin.ns4kafka.controller.generic;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.service.NamespaceService;
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
