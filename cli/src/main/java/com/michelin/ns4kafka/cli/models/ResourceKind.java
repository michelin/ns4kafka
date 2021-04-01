package com.michelin.ns4kafka.cli.models;

public enum ResourceKind {
    NAMESPACE("Namespace"),
    ACCESSCONTROLENTRY("AccessControlEntry"),
    TOPIC("Topic"),
    CONNECTOR("Connector");

    public final String value;

    private ResourceKind(String value) {
        this.value = value;
    }

}
