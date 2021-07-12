package com.michelin.ns4kafka.controllers;

import lombok.Getter;

import java.util.List;

public class ResourceConflictException extends RuntimeException {
    @Getter
    private final List<String> conflicts;

    @Getter
    private final String kind;
    @Getter
    private final String name;

    public ResourceConflictException(List<String> conflicts, String kind, String name) {
        this.conflicts = conflicts;
        this.kind = kind;
        this.name = name;
    }

}
