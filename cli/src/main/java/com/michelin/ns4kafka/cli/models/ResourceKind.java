package com.michelin.ns4kafka.cli.models;

import java.util.HashMap;
import java.util.Map;

public enum ResourceKind {
    NAMESPACE("Namespace"),
    ACCESSCONTROLENTRY("AccessControlEntry"),
    TOPIC("Topic"),
    CONNECTOR("Connector");

    public final String value;

    private static final Map<String, ResourceKind> BY_VALUE = new HashMap<>();
    static {
        for (ResourceKind r: values()) {
            BY_VALUE.put(r.value, r);
        }
    }
    private ResourceKind(String value) {
        this.value = value;
    }
    public static ResourceKind resourceKindFromValue(String value) {
        return BY_VALUE.get(value);
    }
}
