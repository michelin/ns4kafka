package com.michelin.ns4kafka.models;

import lombok.*;

import java.util.List;

@NoArgsConstructor
@Getter
@Setter
public class Namespace {
    private String name;
    private List<ResourceSecurityPolicy> policies;
    private int diskQuota;

}
