package com.michelin.ns4kafka.cli.models;

public enum SchemaCompatibility {
    DEFAULT,
    BACKWARD,
    BACKWARD_TRANSITIVE,
    FORWARD,
    FORWARD_TRANSITIVE,
    FULL,
    FULL_TRANSITIVE,
    NONE
}
