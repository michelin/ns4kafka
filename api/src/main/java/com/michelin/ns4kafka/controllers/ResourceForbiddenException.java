package com.michelin.ns4kafka.controllers;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class ResourceForbiddenException extends RuntimeException {

    @Getter
    private final String kind;
    @Getter
    private final String name;

}
