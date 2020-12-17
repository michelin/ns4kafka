package com.michelin.ns4kafka.models;

import lombok.*;

import java.util.List;

@NoArgsConstructor
@Getter
@Setter
public class Namespace {
    private String name;
    private String cluster;
    private List<ResourceSecurityPolicy> policies;
    private int diskQuota;
    private String defaultUser="FDW_OLS_01";
    //private List<User> users; MVP 35


}
