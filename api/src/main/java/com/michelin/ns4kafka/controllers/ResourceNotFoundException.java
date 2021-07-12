package com.michelin.ns4kafka.controllers;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class ResourceNotFoundException extends RuntimeException {

    private static final long serialVersionUID = -3259562178153814829L;

    @Getter
    private final String kind;
    @Getter
    private final String name;
}
